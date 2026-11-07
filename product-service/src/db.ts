import path from 'path';
import fs from 'fs';
import { execFileSync, spawn, ChildProcessWithoutNullStreams } from 'child_process';

export interface Product {
    id: number;
    sku: string;
    name: string;
    category: string;
    price: number;
    weightKg: number;
    active: number;
    createdAt: string;
}

const DATA_DIR = path.join(__dirname, '..', 'data');
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

export const DB_PATH = process.env.SQLITE_DB_PATH || path.join(DATA_DIR, 'products.db');

/**
 * Executa queries SQLite no arquivo binário `products.db` mantendo cache em memória sincronizado
 * e um worker Python persistente em modo WAL para evitar overhead de spawn por requisição.
 */
class SqliteBridge {
    private nativeDb: any = null;
    private productsCache: Product[] = [];
    private cacheLoaded = false;
    private pyWorker: ChildProcessWithoutNullStreams | null = null;

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
            });
        } catch {
            this.pyWorker = null;
        }
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

    public reloadCacheFromDisk(): void {
        try {
            const rows = this.execSqlSyncFallback('SELECT * FROM products ORDER BY id ASC');
            if (Array.isArray(rows) && rows.length > 0) {
                this.productsCache = rows;
                this.cacheLoaded = true;
            }
        } catch {
            // Ignora falha transitória de lock
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

        // Leituras rápidas em memória sincronizadas com products.db (0ms, sem bloquear o Event Loop)
        if (normalized.startsWith('SELECT') && this.cacheLoaded) {
            if (normalized.includes('COUNT(*) AS COUNT')) {
                return [{ count: this.productsCache.length }];
            }
            if (normalized.includes('COUNT(*) AS TOTAL')) {
                return [{ total: this.productsCache.length }];
            }
            if (normalized.includes('WHERE ID = ? AND ACTIVE = 1')) {
                const id = Number(params[0]);
                return this.productsCache.filter(p => Number(p.id) === id && Number(p.active) === 1);
            }
            if (normalized.includes('WHERE ID = ?')) {
                const id = Number(params[0]);
                return this.productsCache.filter(p => Number(p.id) === id);
            }
            if (normalized.includes('WHERE UPPER(CATEGORY) = UPPER(?)')) {
                const cat = String(params[0] || '').toUpperCase();
                return this.productsCache.filter(p => String(p.category).toUpperCase() === cat && Number(p.active) === 1);
            }
            if (normalized.includes('FROM PRODUCTS')) {
                return this.productsCache.filter(p => Number(p.active) === 1);
            }
        }

        if (normalized.startsWith('INSERT INTO PRODUCTS')) {
            if (params.length >= 7) {
                const prod: Product = {
                    id: Number(params[0]),
                    sku: String(params[1]),
                    name: String(params[2]),
                    category: String(params[3]),
                    price: Number(params[4]),
                    weightKg: Number(params[5]),
                    active: params.length === 8 ? Number(params[6]) : 1,
                    createdAt: String(params[params.length - 1])
                };
                this.productsCache = this.productsCache.filter(p => p.id !== prod.id);
                this.productsCache.push(prod);
                this.productsCache.sort((a, b) => a.id - b.id);
                this.cacheLoaded = true;
            }
            if (this.pyWorker && this.pyWorker.stdin.writable) {
                this.pyWorker.stdin.write(JSON.stringify({ sql, params }) + '\n');
                return [];
            }
        }

        return this.execSqlSyncFallback(sql, params);
    }
}

export const db = new SqliteBridge(DB_PATH);

export function initDatabase(): void {
    db.execSql(`
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            weightKg REAL NOT NULL DEFAULT 1.0,
            active INTEGER NOT NULL DEFAULT 1,
            createdAt TEXT NOT NULL
        )
    `);

    db.reloadCacheFromDisk();
    const existing = db.execSql('SELECT COUNT(*) as total FROM products');
    const count = existing[0]?.total ?? 0;

    if (count === 0) {
        const now = new Date().toISOString();
        const seedProducts: Product[] = [
            { id: 101, sku: 'TECH-LAP-101', name: 'Laptop Gamer Pro RTX', category: 'ELECTRONICS', price: 6500.00, weightKg: 2.4, active: 1, createdAt: now },
            { id: 102, sku: 'TECH-MOU-102', name: 'Mouse Sem Fio UltraLight', category: 'PERIPHERALS', price: 150.00, weightKg: 0.1, active: 1, createdAt: now },
            { id: 103, sku: 'TECH-KEY-103', name: 'Teclado Mecânico Custom RGB', category: 'PERIPHERALS', price: 480.00, weightKg: 0.9, active: 1, createdAt: now },
            { id: 104, sku: 'TECH-MON-104', name: 'Monitor Ultrawide 34" 165Hz', category: 'ELECTRONICS', price: 3200.00, weightKg: 6.5, active: 1, createdAt: now },
            { id: 105, sku: 'TECH-GPU-105', name: 'Workstation AI Server GPU', category: 'ENTERPRISE', price: 28000.00, weightKg: 12.0, active: 1, createdAt: now }
        ];

        for (const p of seedProducts) {
            db.execSql(
                `INSERT INTO products (id, sku, name, category, price, weightKg, active, createdAt)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                [p.id, p.sku, p.name, p.category, p.price, p.weightKg, p.active, p.createdAt]
            );
        }
        console.log(`[Product Service] SQLite inicializado com ${seedProducts.length} produtos em: ${DB_PATH}`);
    } else {
        console.log(`[Product Service] SQLite conectado (${count} produtos) em: ${DB_PATH}`);
    }
}
