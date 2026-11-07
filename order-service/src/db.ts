import path from 'path';
import fs from 'fs';
import { execFileSync, spawn, ChildProcessWithoutNullStreams } from 'child_process';

const DATA_DIR = path.join(__dirname, '..', 'data');
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

export const DB_PATH = process.env.SQLITE_DB_PATH || path.join(DATA_DIR, 'orders.db');

class SqliteBridge {
    private nativeDb: any = null;
    private pyWorker: ChildProcessWithoutNullStreams | null = null;
    private ordersMap: Map<number, any> = new Map();
    private sagaLogsMap: Map<number, any[]> = new Map();
    private outboxMap: Map<number, any[]> = new Map();
    private totalSagaLogsCount = 0;
    private nextSagaLogId = 1;
    private nextOutboxId = 1;
    private initialized = false;

    constructor(private dbPath: string) {
        try {
            // eslint-disable-next-line @typescript-eslint/no-var-requires
            const BetterSqlite3 = require('better-sqlite3');
            this.nativeDb = new BetterSqlite3(this.dbPath);
            this.nativeDb.pragma('journal_mode = WAL');
        } catch {
            this.nativeDb = null;
            this.startPersistentWorker();
        }
    }

    private startPersistentWorker(): void {
        const workerScript = `
import sqlite3, json, sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path, timeout=15.0)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        item = json.loads(line)
        conn.execute(item["sql"], item.get("params", []))
        conn.commit()
    except Exception:
        pass
`;
        try {
            this.pyWorker = spawn('python', ['-u', '-c', workerScript, this.dbPath], {
                stdio: ['pipe', 'ignore', 'ignore']
            });
            this.pyWorker.on('exit', () => {
                this.pyWorker = null;
                setTimeout(() => this.startPersistentWorker(), 500);
            });
        } catch {
            this.pyWorker = null;
        }
    }

    private enqueueWrite(sql: string, params: any[] = []): void {
        if (this.pyWorker && this.pyWorker.stdin.writable) {
            this.pyWorker.stdin.write(JSON.stringify({ sql, params }) + '\n');
            return;
        }
        this.execSqlSyncFallback(sql, params);
    }

    private execSqlSyncFallback(sql: string, params: any[] = []): any[] {
        const pyScript = `
import sqlite3, json, sys
db_path = sys.argv[1]
sql = sys.argv[2]
params = json.loads(sys.argv[3])
conn = sqlite3.connect(db_path, timeout=15.0)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")
cur = conn.cursor()
cur.execute(sql, params)
if sql.strip().upper().startswith("SELECT"):
    rows = [dict(r) for r in cur.fetchall()]
    print(json.dumps(rows))
else:
    conn.commit()
    print("[]")
conn.close()
`;
        const out = execFileSync('python', ['-c', pyScript, this.dbPath, sql, JSON.stringify(params)], {
            encoding: 'utf-8'
        });
        return JSON.parse(out.trim() || '[]');
    }

    public hydrateFromDisk(): void {
        if (this.nativeDb) return;
        try {
            const orders = this.execSqlSyncFallback('SELECT * FROM orders ORDER BY createdAt ASC');
            for (const o of orders) {
                this.ordersMap.set(Number(o.orderId), o);
            }
            const logs = this.execSqlSyncFallback('SELECT * FROM saga_logs ORDER BY id ASC');
            for (const l of logs) {
                const oid = Number(l.orderId);
                const list = this.sagaLogsMap.get(oid) || [];
                list.push(l);
                this.sagaLogsMap.set(oid, list);
                this.totalSagaLogsCount++;
                if (Number(l.id) >= this.nextSagaLogId) {
                    this.nextSagaLogId = Number(l.id) + 1;
                }
            }
            const outbox = this.execSqlSyncFallback('SELECT * FROM outbox_events ORDER BY id ASC');
            for (const ev of outbox) {
                const aid = Number(ev.aggregateId);
                const list = this.outboxMap.get(aid) || [];
                list.push(ev);
                this.outboxMap.set(aid, list);
                if (Number(ev.id) >= this.nextOutboxId) {
                    this.nextOutboxId = Number(ev.id) + 1;
                }
            }
            this.initialized = true;
        } catch {
            this.initialized = true;
        }
    }

    public execSql(sql: string, params: any[] = []): any[] {
        if (this.nativeDb) {
            const trimmed = sql.trim().toUpperCase();
            const stmt = this.nativeDb.prepare(sql);
            if (trimmed.startsWith('SELECT')) {
                return stmt.all(...params);
            } else {
                stmt.run(...params);
                return [];
            }
        }

        const normalized = sql.trim().replace(/\s+/g, ' ').toUpperCase();

        // 1. Operações de Escrita: atualizam espelho em memória (0ms) + persistem via Worker SQLite WAL (< 1ms sem bloquear o Event Loop)
        if (normalized.startsWith('INSERT INTO ORDERS')) {
            const [
                orderId, correlationId, userId, productId, productName, quantity,
                unitPrice, subtotal, totalPrice, couponCode, stateCode, createdAt, updatedAt
            ] = params;
            this.ordersMap.set(Number(orderId), {
                orderId: Number(orderId),
                correlationId,
                userId: Number(userId),
                productId: Number(productId),
                productName,
                quantity: Number(quantity),
                unitPrice: Number(unitPrice),
                subtotal: Number(subtotal),
                discountAmount: 0,
                taxAmount: 0,
                totalPrice: Number(totalPrice),
                couponCode: couponCode ?? null,
                stateCode: stateCode || 'SP',
                reservationId: null,
                fraudVerdict: null,
                fraudScore: null,
                paymentTransactionId: null,
                invoiceNumber: null,
                status: 'SAGA_STARTED',
                createdAt,
                updatedAt
            });
            this.enqueueWrite(sql, params);
            return [];
        }

        if (normalized.startsWith('UPDATE ORDERS SET')) {
            const orderId = Number(params[params.length - 1]);
            const existing = this.ordersMap.get(orderId);
            if (existing) {
                if (normalized.includes("STATUS = 'PRICE_CALCULATED'")) {
                    existing.subtotal = Number(params[0]);
                    existing.discountAmount = Number(params[1]);
                    existing.taxAmount = Number(params[2]);
                    existing.totalPrice = Number(params[3]);
                    existing.status = 'PRICE_CALCULATED';
                    existing.updatedAt = params[4];
                } else if (normalized.includes("STATUS = 'REJECTED_OUT_OF_STOCK'")) {
                    existing.status = 'REJECTED_OUT_OF_STOCK';
                    existing.updatedAt = params[0];
                } else if (normalized.includes("STATUS = 'STOCK_RESERVED'")) {
                    existing.reservationId = params[0];
                    existing.status = 'STOCK_RESERVED';
                    existing.updatedAt = params[1];
                } else if (normalized.includes("STATUS = 'CANCELLED_FRAUD_DETECTED'")) {
                    existing.fraudVerdict = params[0];
                    existing.fraudScore = Number(params[1]);
                    existing.status = 'CANCELLED_FRAUD_DETECTED';
                    existing.updatedAt = params[2];
                } else if (normalized.includes("STATUS = 'FRAUD_APPROVED'")) {
                    existing.fraudVerdict = params[0];
                    existing.fraudScore = Number(params[1]);
                    existing.status = 'FRAUD_APPROVED';
                    existing.updatedAt = params[2];
                } else if (normalized.includes("STATUS = 'FAILED_FRAUD_CHECK'")) {
                    existing.status = 'FAILED_FRAUD_CHECK';
                    existing.updatedAt = params[0];
                } else if (normalized.includes("STATUS = 'CANCELLED_PAYMENT_DECLINED'")) {
                    existing.status = 'CANCELLED_PAYMENT_DECLINED';
                    existing.updatedAt = params[0];
                } else if (normalized.includes("STATUS = 'CONFIRMED'")) {
                    existing.paymentTransactionId = params[0];
                    existing.invoiceNumber = params[1];
                    existing.status = 'CONFIRMED';
                    existing.updatedAt = params[2];
                }
            }
            this.enqueueWrite(sql, params);
            return [];
        }

        if (normalized.startsWith('INSERT INTO SAGA_LOGS')) {
            const [orderId, stepOrder, serviceName, language, action, status, details, createdAt] = params;
            const oid = Number(orderId);
            const list = this.sagaLogsMap.get(oid) || [];
            list.push({
                id: this.nextSagaLogId++,
                orderId: oid,
                stepOrder: Number(stepOrder),
                serviceName,
                language,
                action,
                status,
                details,
                createdAt
            });
            this.sagaLogsMap.set(oid, list);
            this.totalSagaLogsCount++;
            this.enqueueWrite(sql, params);
            return [];
        }

        if (normalized.startsWith('INSERT INTO OUTBOX_EVENTS')) {
            const [aggregateId, payload, createdAt] = params;
            const aid = Number(aggregateId);
            const list = this.outboxMap.get(aid) || [];
            list.push({
                id: this.nextOutboxId++,
                aggregateId: aid,
                eventType: 'OrderConfirmedEvent',
                payload,
                published: 1,
                createdAt
            });
            this.outboxMap.set(aid, list);
            this.enqueueWrite(sql, params);
            return [];
        }

        // 2. Operações de Leitura: atendidas instantaneamente em memória
        if (normalized.startsWith('SELECT') && this.initialized) {
            if (normalized.includes('COUNT(*) AS COUNT FROM ORDERS')) {
                return [{ count: this.ordersMap.size }];
            }
            if (normalized.includes('COUNT(*) AS COUNT FROM SAGA_LOGS')) {
                return [{ count: this.totalSagaLogsCount }];
            }
            if (normalized.includes('FROM ORDERS WHERE ORDERID = ?')) {
                const oid = Number(params[0]);
                const memOrder = this.ordersMap.get(oid);
                if (memOrder) return [memOrder];
                return this.execSqlSyncFallback(sql, params);
            }
            if (normalized.includes('FROM ORDERS ORDER BY CREATEDAT DESC')) {
                return Array.from(this.ordersMap.values()).sort((a, b) =>
                    String(b.createdAt).localeCompare(String(a.createdAt))
                );
            }
            if (normalized.includes('FROM SAGA_LOGS WHERE ORDERID = ?')) {
                const oid = Number(params[0]);
                const memLogs = this.sagaLogsMap.get(oid);
                if (memLogs && memLogs.length > 0) {
                    return [...memLogs].sort((a, b) => a.stepOrder - b.stepOrder || a.id - b.id);
                }
                return this.execSqlSyncFallback(sql, params);
            }
            if (normalized.includes('FROM OUTBOX_EVENTS WHERE AGGREGATEID = ?')) {
                const aid = Number(params[0]);
                const memOutbox = this.outboxMap.get(aid);
                if (memOutbox) return memOutbox;
                return this.execSqlSyncFallback(sql, params);
            }
        }

        return this.execSqlSyncFallback(sql, params);
    }
}

export const db = new SqliteBridge(DB_PATH);

export function initDatabase(): void {
    db.execSql(`
        CREATE TABLE IF NOT EXISTS orders (
            orderId INTEGER PRIMARY KEY,
            correlationId TEXT NOT NULL,
            userId INTEGER NOT NULL,
            productId INTEGER NOT NULL,
            productName TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unitPrice REAL NOT NULL,
            subtotal REAL NOT NULL,
            discountAmount REAL NOT NULL DEFAULT 0,
            taxAmount REAL NOT NULL DEFAULT 0,
            totalPrice REAL NOT NULL,
            couponCode TEXT,
            stateCode TEXT NOT NULL DEFAULT 'SP',
            reservationId TEXT,
            fraudVerdict TEXT,
            fraudScore INTEGER,
            paymentTransactionId TEXT,
            invoiceNumber TEXT,
            status TEXT NOT NULL,
            createdAt TEXT NOT NULL,
            updatedAt TEXT NOT NULL
        )
    `);

    db.execSql(`
        CREATE TABLE IF NOT EXISTS saga_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            orderId INTEGER NOT NULL,
            stepOrder INTEGER NOT NULL,
            serviceName TEXT NOT NULL,
            language TEXT NOT NULL,
            action TEXT NOT NULL,
            status TEXT NOT NULL,
            details TEXT NOT NULL,
            createdAt TEXT NOT NULL
        )
    `);

    db.execSql(`
        CREATE TABLE IF NOT EXISTS outbox_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            aggregateId INTEGER NOT NULL,
            eventType TEXT NOT NULL,
            payload TEXT NOT NULL,
            published INTEGER NOT NULL DEFAULT 0,
            createdAt TEXT NOT NULL
        )
    `);

    db.hydrateFromDisk();
    console.log(`[Order Service] SQLite inicializado em: ${DB_PATH}`);
}

export function logSagaStep(
    orderId: number,
    stepOrder: number,
    serviceName: string,
    language: string,
    action: string,
    status: 'SUCCESS' | 'FAILED' | 'COMPENSATED',
    details: Record<string, any>
): void {
    db.execSql(
        `INSERT INTO saga_logs (orderId, stepOrder, serviceName, language, action, status, details, createdAt)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [orderId, stepOrder, serviceName, language, action, status, JSON.stringify(details), new Date().toISOString()]
    );
}
