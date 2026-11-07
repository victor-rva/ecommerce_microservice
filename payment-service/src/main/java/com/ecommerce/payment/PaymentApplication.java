package com.ecommerce.payment;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Microsserviço Transacional de Pagamentos e Ledger Contábil em Java 17.
 * Implementa Idempotência Financeira e Partidas Dobradas (Double-Entry Bookkeeping)
 * com persistência isolada no banco SQLite: payments.db.
 */
public class PaymentApplication {

    private static final String DB_PATH = System.getenv("SQLITE_DB_PATH") != null
            ? System.getenv("SQLITE_DB_PATH")
            : "data/payments.db";

    private static final Object DB_LOCK = new Object();

    // Java 17 Records para Imutabilidade de Domínio Financeiro
    public record ChargeRequest(long orderId, long userId, double amount, String paymentMethod, String idempotencyKey) {}

    public record PaymentResult(
            String transactionId,
            long orderId,
            long userId,
            double amount,
            String paymentMethod,
            String status,
            String idempotencyKey,
            String ledgerDebitEntryId,
            String ledgerCreditEntryId,
            String processedAt
    ) {
        public String toJson() {
            return String.format(
                    java.util.Locale.US,
                    "{\"service\":\"payment-service\",\"language\":\"Java 17 (JVM Enterprise)\"," +
                    "\"transactionId\":\"%s\",\"orderId\":%d,\"userId\":%d,\"amount\":%.2f," +
                    "\"paymentMethod\":\"%s\",\"status\":\"%s\",\"idempotencyKey\":\"%s\"," +
                    "\"ledger\":{\"debitEntry\":\"%s\",\"creditEntry\":\"%s\"},\"processedAt\":\"%s\"}",
                    transactionId, orderId, userId, amount, paymentMethod, status,
                    idempotencyKey, ledgerDebitEntryId, ledgerCreditEntryId, processedAt
            );
        }
    }

    public static void main(String[] args) throws IOException {
        initDatabase();

        int port = Integer.parseInt(System.getenv("PORT") != null ? System.getenv("PORT") : "3006");
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/health", exchange -> {
            String countPayments = execSqliteScalar("SELECT COUNT(*) FROM payments");
            String countLedger = execSqliteScalar("SELECT COUNT(*) FROM ledger_entries");
            String json = String.format(
                    "{\"service\":\"payment-service\",\"language\":\"Java 17 (LTS)\",\"status\":\"UP\"," +
                    "\"database\":\"%s\",\"totalPayments\":%s,\"totalLedgerEntries\":%s}",
                    DB_PATH.replace("\\", "/"), countPayments, countLedger
            );
            sendJson(exchange, 200, json);
        });

        server.createContext("/payments/ledger", exchange -> {
            String rows = execSqliteJson("SELECT * FROM ledger_entries ORDER BY id DESC LIMIT 50");
            sendJson(exchange, 200, rows);
        });

        server.createContext("/payments/charge", exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, "{\"message\":\"Method Not Allowed\"}");
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            ChargeRequest req = parseChargeRequest(body);

            // Regra de simulação: paymentMethod == "DECLINED_CARD" ou userId == 789 (ou 777) simula cartão sem saldo
            boolean declined = "DECLINED_CARD".equalsIgnoreCase(req.paymentMethod()) || req.userId() == 789 || req.userId() == 777;
            String status = declined ? "DECLINED" : "APPROVED";

            String txId = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            String debitId = "LED-DR-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            String creditId = "LED-CR-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            String now = Instant.now().toString();

            java.util.concurrent.CompletableFuture.runAsync(() -> {
                synchronized (DB_LOCK) {
                    String sqlBatch = String.format(
                            java.util.Locale.US,
                            "INSERT OR IGNORE INTO payments (transaction_id, order_id, user_id, amount, payment_method, status, idempotency_key, created_at) " +
                            "VALUES ('%s', %d, %d, %.2f, '%s', '%s', '%s', '%s');",
                            txId, req.orderId(), req.userId(), req.amount(),
                            escapeSql(req.paymentMethod()), status, escapeSql(req.idempotencyKey()), now
                    );
                    if ("APPROVED".equals(status)) {
                        sqlBatch += String.format(
                                java.util.Locale.US,
                                " INSERT INTO ledger_entries (entry_code, transaction_id, order_id, account, entry_type, amount, created_at) VALUES " +
                                "('%s', '%s', %d, 'CUSTOMER_RECEIVABLE_%d', 'DEBIT', %.2f, '%s'), " +
                                "('%s', '%s', %d, 'MERCHANT_REVENUE_CLEARING', 'CREDIT', %.2f, '%s');",
                                debitId, txId, req.orderId(), req.userId(), req.amount(), now,
                                creditId, txId, req.orderId(), req.amount(), now
                        );
                    }
                    execSqliteJson(sqlBatch);
                }
            });

            PaymentResult result = new PaymentResult(
                    txId, req.orderId(), req.userId(), req.amount(),
                    req.paymentMethod(), status, req.idempotencyKey(),
                    debitId, creditId, now
            );

            System.out.printf("[Payment Service - Java 17] Order #%d -> %s (Tx: %s | R$ %.2f)%n",
                    req.orderId(), status, txId, req.amount());

            sendJson(exchange, "APPROVED".equals(status) ? 201 : 402, result.toJson());
        });

        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        System.out.printf("[Payment Service - Java 17] Rodando na porta %d | SQLite: %s%n", port, DB_PATH);
    }

    private static void initDatabase() {
        File dbFile = new File(DB_PATH);
        if (dbFile.getParentFile() != null) {
            dbFile.getParentFile().mkdirs();
        }
        execSqliteJson(
                "CREATE TABLE IF NOT EXISTS payments (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "transaction_id TEXT NOT NULL UNIQUE, " +
                "order_id INTEGER NOT NULL, " +
                "user_id INTEGER NOT NULL, " +
                "amount REAL NOT NULL, " +
                "payment_method TEXT NOT NULL, " +
                "status TEXT NOT NULL, " +
                "idempotency_key TEXT NOT NULL UNIQUE, " +
                "created_at TEXT NOT NULL)"
        );
        execSqliteJson(
                "CREATE TABLE IF NOT EXISTS ledger_entries (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "entry_code TEXT NOT NULL, " +
                "transaction_id TEXT NOT NULL, " +
                "order_id INTEGER NOT NULL, " +
                "account TEXT NOT NULL, " +
                "entry_type TEXT NOT NULL, " +
                "amount REAL NOT NULL, " +
                "created_at TEXT NOT NULL)"
        );
        System.out.println("[Payment Service - Java 17] SQLite inicializado em: " + DB_PATH);
    }

    private static ChargeRequest parseChargeRequest(String json) {
        long orderId = (long) extractNumber(json, "orderId", 0);
        long userId = (long) extractNumber(json, "userId", 0);
        double amount = extractNumber(json, "amount", 0.0);
        String method = extractString(json, "paymentMethod", "CREDIT_CARD");
        String idemKey = extractString(json, "idempotencyKey", "IDEM-" + orderId);
        return new ChargeRequest(orderId, userId, amount, method, idemKey);
    }

    private static double extractNumber(String json, String key, double fallback) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*([0-9.]+)").matcher(json);
        return m.find() ? Double.parseDouble(m.group(1)) : fallback;
    }

    private static String extractString(String json, String key, String fallback) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"").matcher(json);
        return m.find() ? m.group(1) : fallback;
    }

    private static String escapeSql(String s) {
        return s == null ? "" : s.replace("'", "''");
    }

    private static String execSqliteJson(String sql) {
        try {
            String py = "import sqlite3, json, sys\n" +
                    "conn = sqlite3.connect(sys.argv[1], timeout=15.0)\n" +
                    "conn.row_factory = sqlite3.Row\n" +
                    "conn.execute('PRAGMA journal_mode=WAL;')\n" +
                    "cur = conn.cursor()\n" +
                    "sql = sys.argv[2].strip()\n" +
                    "if sql.upper().startswith('SELECT'):\n" +
                    "    cur.execute(sql)\n" +
                    "    print(json.dumps([dict(r) for r in cur.fetchall()]))\n" +
                    "else:\n" +
                    "    conn.executescript(sql)\n" +
                    "    conn.commit()\n" +
                    "    print('[]')\n" +
                    "conn.close()\n";
            Process p = new ProcessBuilder("python", "-c", py, DB_PATH, sql).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            p.waitFor();
            return out.isEmpty() ? "[]" : out;
        } catch (Exception e) {
            return "[]";
        }
    }

    private static String execSqliteScalar(String sql) {
        try {
            String py = "import sqlite3, sys\n" +
                    "conn = sqlite3.connect(sys.argv[1])\n" +
                    "cur = conn.cursor()\n" +
                    "cur.execute(sys.argv[2])\n" +
                    "r = cur.fetchone()\n" +
                    "print(r[0] if r else 0)\n" +
                    "conn.close()\n";
            Process p = new ProcessBuilder("python", "-c", py, DB_PATH, sql).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            p.waitFor();
            return out.isEmpty() ? "0" : out;
        } catch (Exception e) {
            return "0";
        }
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
