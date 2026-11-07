import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

PORT = int(os.environ.get("PORT", "3004"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(DATA_DIR, "inventory.db"))


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_database() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_items (
            product_id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL,
            product_name TEXT NOT NULL,
            available_qty INTEGER NOT NULL,
            reserved_qty INTEGER NOT NULL DEFAULT 0,
            reorder_threshold INTEGER NOT NULL DEFAULT 5,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_reservations (
            reservation_id TEXT PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute("SELECT COUNT(*) as total FROM stock_items")
    if cur.fetchone()["total"] == 0:
        now = datetime.now(timezone.utc).isoformat()
        seeds = [
            (101, "TECH-LAP-101", "Laptop Gamer Pro RTX", 25, 0, 5, now),
            (102, "TECH-MOU-102", "Mouse Sem Fio UltraLight", 120, 0, 15, now),
            (103, "TECH-KEY-103", "Teclado Mecânico Custom RGB", 45, 0, 10, now),
            (104, "TECH-MON-104", "Monitor Ultrawide 34 165Hz", 15, 0, 4, now),
            (105, "TECH-GPU-105", "Workstation AI Server GPU", 5, 0, 2, now),
        ]
        cur.executemany(
            """
            INSERT INTO stock_items (product_id, sku, product_name, available_qty, reserved_qty, reorder_threshold, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            seeds,
        )
        conn.commit()
        print(f"[Inventory Service - Python] SQLite inicializado com {len(seeds)} itens em: {DB_PATH}")
    else:
        print(f"[Inventory Service - Python] SQLite conectado em: {DB_PATH}")
    conn.close()


def enrich_stock_item(row: sqlite3.Row) -> dict:
    item = dict(row)
    item["needs_restock"] = item["available_qty"] <= item["reorder_threshold"]
    item["total_physical_qty"] = item["available_qty"] + item["reserved_qty"]
    return item


def reserve_stock(order_id: int, product_id: int, quantity: int) -> tuple[int, dict]:
    if quantity <= 0:
        return 400, {"message": "Quantidade deve ser maior que zero"}

    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        cur.execute("SELECT * FROM stock_items WHERE product_id = ?", (product_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return 404, {"message": f"Estoque não cadastrado para o produto {product_id}"}

        available = row["available_qty"]
        if available < quantity:
            conn.rollback()
            return 409, {
                "message": "Saldo de estoque insuficiente",
                "productId": product_id,
                "requestedQty": quantity,
                "availableQty": available,
            }

        reservation_id = f"RES-{uuid.uuid4().hex[:10].upper()}"
        now = datetime.now(timezone.utc).isoformat()

        cur.execute(
            """
            UPDATE stock_items
            SET available_qty = available_qty - ?,
                reserved_qty = reserved_qty + ?,
                updated_at = ?
            WHERE product_id = ?
            """,
            (quantity, quantity, now, product_id),
        )
        cur.execute(
            """
            INSERT INTO stock_reservations (reservation_id, order_id, product_id, quantity, status, reason, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'RESERVED', 'SAGA_CHECKOUT_LOCK', ?, ?)
            """,
            (reservation_id, order_id, product_id, quantity, now, now),
        )
        conn.commit()

        print(
            f"[Inventory Service - Python] Reserva {reservation_id} criada (Order #{order_id}: {quantity}x Produto {product_id})"
        )
        return 201, {
            "service": "inventory-service",
            "language": "Python 3.11",
            "reservationId": reservation_id,
            "orderId": order_id,
            "productId": product_id,
            "reservedQuantity": quantity,
            "remainingAvailableQty": available - quantity,
            "status": "RESERVED",
        }
    finally:
        conn.close()


def confirm_reservation(reservation_id: str, order_id: int) -> tuple[int, dict]:
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        cur.execute("SELECT * FROM stock_reservations WHERE reservation_id = ?", (reservation_id,))
        res_row = cur.fetchone()
        if not res_row:
            conn.rollback()
            return 404, {"message": f"Reserva {reservation_id} não encontrada"}

        if res_row["status"] != "RESERVED":
            conn.rollback()
            return 200, {
                "reservationId": reservation_id,
                "status": res_row["status"],
                "note": "Reserva já processada anteriormente (idempotente)",
            }

        qty = res_row["quantity"]
        product_id = res_row["product_id"]
        now = datetime.now(timezone.utc).isoformat()

        cur.execute(
            """
            UPDATE stock_items
            SET reserved_qty = MAX(0, reserved_qty - ?),
                updated_at = ?
            WHERE product_id = ?
            """,
            (qty, now, product_id),
        )
        cur.execute(
            """
            UPDATE stock_reservations
            SET status = 'CONFIRMED', reason = 'PAYMENT_APPROVED_COMMIT', updated_at = ?
            WHERE reservation_id = ?
            """,
            (now, reservation_id),
        )
        conn.commit()
        print(f"[Inventory Service - Python] Reserva {reservation_id} CONFIRMADA (baixa definitiva Order #{order_id})")
        return 200, {
            "service": "inventory-service",
            "reservationId": reservation_id,
            "orderId": order_id,
            "status": "CONFIRMED",
        }
    finally:
        conn.close()


def release_reservation(reservation_id: str, order_id: int, reason: str) -> tuple[int, dict]:
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        cur.execute("SELECT * FROM stock_reservations WHERE reservation_id = ?", (reservation_id,))
        res_row = cur.fetchone()
        if not res_row:
            conn.rollback()
            return 404, {"message": f"Reserva {reservation_id} não encontrada"}

        if res_row["status"] != "RESERVED":
            conn.rollback()
            return 200, {
                "reservationId": reservation_id,
                "status": res_row["status"],
                "note": "Reserva já compensada anteriormente",
            }

        qty = res_row["quantity"]
        product_id = res_row["product_id"]
        now = datetime.now(timezone.utc).isoformat()

        cur.execute(
            """
            UPDATE stock_items
            SET available_qty = available_qty + ?,
                reserved_qty = MAX(0, reserved_qty - ?),
                updated_at = ?
            WHERE product_id = ?
            """,
            (qty, qty, now, product_id),
        )
        cur.execute(
            """
            UPDATE stock_reservations
            SET status = 'RELEASED_COMPENSATED', reason = ?, updated_at = ?
            WHERE reservation_id = ?
            """,
            (reason, now, reservation_id),
        )
        conn.commit()
        print(
            f"[Inventory Service - Python] [SAGA COMPENSATION] Reserva {reservation_id} estornada ao estoque (Motivo: {reason})"
        )
        return 200, {
            "service": "inventory-service",
            "language": "Python 3.11",
            "reservationId": reservation_id,
            "orderId": order_id,
            "productId": product_id,
            "restoredQuantity": qty,
            "status": "RELEASED_COMPENSATED",
            "reason": reason,
        }
    finally:
        conn.close()


class InventoryHTTPHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict | list) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if path == "/health":
            conn = get_connection()
            total_items = conn.execute("SELECT COUNT(*) as c FROM stock_items").fetchone()["c"]
            total_res = conn.execute("SELECT COUNT(*) as c FROM stock_reservations").fetchone()["c"]
            conn.close()
            self._send_json(
                200,
                {
                    "service": "inventory-service",
                    "language": "Python 3.11",
                    "status": "UP",
                    "database": DB_PATH,
                    "trackedProducts": total_items,
                    "totalReservations": total_res,
                },
            )
            return

        if path == "/inventory":
            conn = get_connection()
            rows = conn.execute("SELECT * FROM stock_items ORDER BY product_id ASC").fetchall()
            conn.close()
            self._send_json(200, [enrich_stock_item(r) for r in rows])
            return

        if path == "/inventory/reservations":
            conn = get_connection()
            rows = conn.execute("SELECT * FROM stock_reservations ORDER BY created_at DESC LIMIT 50").fetchall()
            conn.close()
            self._send_json(200, [dict(r) for r in rows])
            return

        if path.startswith("/inventory/"):
            try:
                product_id = int(path.split("/")[-1])
            except ValueError:
                self._send_json(400, {"message": "ID de produto inválido"})
                return
            conn = get_connection()
            row = conn.execute("SELECT * FROM stock_items WHERE product_id = ?", (product_id,)).fetchone()
            conn.close()
            if not row:
                self._send_json(404, {"message": "Item de estoque não encontrado"})
                return
            self._send_json(200, enrich_stock_item(row))
            return

        self._send_json(404, {"message": "Rota não encontrada no inventory-service"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        body = self._read_json()

        if path == "/inventory/reserve":
            status, resp = reserve_stock(
                int(body.get("orderId", 0)),
                int(body.get("productId", 0)),
                int(body.get("quantity", 0)),
            )
            self._send_json(status, resp)
            return

        if path == "/inventory/confirm":
            status, resp = confirm_reservation(
                str(body.get("reservationId", "")),
                int(body.get("orderId", 0)),
            )
            self._send_json(status, resp)
            return

        if path == "/inventory/release":
            status, resp = release_reservation(
                str(body.get("reservationId", "")),
                int(body.get("orderId", 0)),
                str(body.get("reason", "SAGA_ROLLBACK")),
            )
            self._send_json(status, resp)
            return

        self._send_json(404, {"message": "Rota POST não encontrada no inventory-service"})


if __name__ == "__main__":
    init_database()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), InventoryHTTPHandler)
    print(f"[Inventory Service - Python 3.11] Rodando na porta {PORT} | SQLite: {DB_PATH}")
    server.serve_forever()
