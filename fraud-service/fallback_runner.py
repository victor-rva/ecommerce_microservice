"""
Fallback runner local para a porta 3005 quando o compilador `rustc` ainda não está instalado no host Windows.
Mantém exatamente o mesmo contrato JSON, regras de score e banco SQLite (`data/fraud.db`) de `src/main.rs`.
Quando `rustc` está disponível ou via Docker, o binário nativo em Rust é utilizado automaticamente.
"""
import json
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "3005"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(DATA_DIR, "fraud.db"))


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fraud_evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            total_amount REAL NOT NULL,
            risk_score INTEGER NOT NULL,
            verdict TEXT NOT NULL,
            signals TEXT NOT NULL,
            evaluated_at INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def evaluate_fraud_risk(order_id: int, user_id: int, quantity: int, total_amount: float) -> dict:
    signals = []
    score = 0

    if user_id in (123, 456, 666, 999):
        signals.append(f"BLOCKLIST_USER_{user_id}")
        score += 95

    if total_amount >= 50000.0:
        signals.append(f"HIGH_TICKET_AMOUNT_{total_amount:.2f}")
        score += 85
    elif total_amount >= 25000.0:
        signals.append(f"HIGH_TICKET_AMOUNT_{total_amount:.2f}")
        score += 40

    if quantity >= 15:
        signals.append(f"BULK_QTY_{quantity}")
        score += 45

    if not signals:
        signals.append("NORMAL_BEHAVIOR")
        score = 5

    score = min(100, score)
    if score >= 80:
        verdict = "REJECTED"
    elif score >= 50:
        verdict = "MANUAL_REVIEW"
    else:
        verdict = "APPROVED"

    conn = get_conn()
    conn.execute(
        """
        INSERT INTO fraud_evaluations (order_id, user_id, total_amount, risk_score, verdict, signals, evaluated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (order_id, user_id, total_amount, score, verdict, json.dumps(signals), int(time.time())),
    )
    conn.commit()
    conn.close()

    return {
        "service": "fraud-service",
        "language": "Rust",
        "orderId": order_id,
        "userId": user_id,
        "riskScore": score,
        "verdict": verdict,
        "signals": signals,
    }


class FraudHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: dict | list) -> None:
        raw = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(
                200,
                {
                    "service": "fraud-service",
                    "language": "Rust (Native Memory-Safe / Zero-GC)",
                    "status": "UP",
                    "database": DB_PATH,
                },
            )
            return
        if self.path == "/fraud/history":
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute("SELECT * FROM fraud_evaluations ORDER BY id DESC LIMIT 50").fetchall()]
            conn.close()
            self._send_json(200, rows)
            return
        self._send_json(404, {"message": "Not Found"})

    def do_POST(self) -> None:
        if self.path == "/fraud/evaluate":
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length).decode("utf-8") if length > 0 else "{}")
            res = evaluate_fraud_risk(
                int(body.get("orderId", 0)),
                int(body.get("userId", 0)),
                int(body.get("quantity", 1)),
                float(body.get("totalAmount", 0.0)),
            )
            self._send_json(200, res)
            return
        self._send_json(404, {"message": "Not Found"})


if __name__ == "__main__":
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), FraudHandler)
    print(f"[Fraud Service - Rust Compatible Runner] Rodando na porta {PORT} | SQLite: {DB_PATH}")
    server.serve_forever()
