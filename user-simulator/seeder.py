"""
Seeder e Resetter dos Bancos de Dados SQLite para o Ecossistema Poliglota de Microsserviços.
Preenche automaticamente os bancos SQLite com:
- Catálogo rico de produtos (product-service -> products.db)
- Saldos de estoque variados para demonstrar reserva em 2 fases e esgotamento (inventory-service -> inventory.db)
- Cupons e alíquotas fiscais (pricing-service -> pricing.db)
- Perfis de usuários simulados (normais, atacadistas/comportamento anômalo, blocklist 123/456 e cartão sem saldo 789)
"""
import argparse
import os
import sqlite3
from datetime import datetime, timezone

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRODUCTS_DB = os.environ.get("PRODUCTS_DB_PATH", os.path.join(ROOT_DIR, "product-service", "data", "products.db"))
INVENTORY_DB = os.environ.get("INVENTORY_DB_PATH", os.path.join(ROOT_DIR, "inventory-service", "data", "inventory.db"))
PRICING_DB = os.environ.get("PRICING_DB_PATH", os.path.join(ROOT_DIR, "pricing-service", "data", "pricing.db"))
ORDERS_DB = os.environ.get("ORDERS_DB_PATH", os.path.join(ROOT_DIR, "order-service", "data", "orders.db"))
FRAUD_DB = os.environ.get("FRAUD_DB_PATH", os.path.join(ROOT_DIR, "fraud-service", "data", "fraud.db"))
PAYMENTS_DB = os.environ.get("PAYMENTS_DB_PATH", os.path.join(ROOT_DIR, "payment-service", "data", "payments.db"))
NOTIFICATIONS_DB = os.environ.get("NOTIFICATIONS_DB_PATH", os.path.join(ROOT_DIR, "notification-service", "data", "notifications.db"))
GATEWAY_DB = os.environ.get("GATEWAY_DB_PATH", os.path.join(ROOT_DIR, "api-gateway", "data", "gateway.db"))
SIMULATOR_DB = os.environ.get("SIMULATOR_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "simulator.db"))

CATALOG_SEEDS = [
    # (id, sku, name, category, price, weightKg, initial_stock, reorder_threshold)
    (101, "TECH-LAP-101", "Laptop Gamer Pro RTX 4080", "ELECTRONICS", 6500.00, 2.4, 35, 5),
    (102, "TECH-MOU-102", "Mouse Sem Fio UltraLight 8K", "PERIPHERALS", 150.00, 0.1, 120, 15),
    (103, "TECH-KEY-103", "Teclado Mecânico Custom RGB", "PERIPHERALS", 480.00, 0.9, 60, 10),
    (104, "TECH-MON-104", "Monitor Ultrawide 34\" 165Hz OLED", "ELECTRONICS", 3200.00, 6.5, 22, 4),
    (105, "TECH-GPU-105", "Workstation AI Server GPU H100", "ENTERPRISE", 28000.00, 12.0, 8, 2),
    (106, "TECH-HDS-106", "Headset Studio Wireless ANC", "AUDIO", 890.00, 0.35, 45, 8),
    (107, "TECH-SSD-107", "SSD NVMe Gen5 2TB 12000MB/s", "STORAGE", 1250.00, 0.05, 50, 10),
    (108, "TECH-CHR-108", "Cadeira Ergonômica Mesh Pro", "OFFICE", 2100.00, 18.5, 14, 3),
    (109, "TECH-CAM-109", "Webcam 4K HDR Streaming", "PERIPHERALS", 620.00, 0.25, 4, 3),  # Estoque baixo para demonstrar esgotamento!
    (110, "TECH-NAS-110", "Storage NAS Enterprise 8-Bay", "ENTERPRISE", 9800.00, 9.2, 6, 2),
    (111, "TECH-MIC-111", "Microfone Condensador Podcast", "AUDIO", 740.00, 0.6, 30, 5),
    (112, "TECH-DK-112", "Docking Station Thunderbolt 4", "ACCESSORIES", 1150.00, 0.4, 3, 2),  # Estoque crítico para disputa!
]

VIRTUAL_USERS = [
    # Clientes regulares (Happy Path)
    {"user_id": 10, "name": "Ana Beatriz", "role": "REGULAR", "state": "SP", "avatar": "👩‍💻", "payment_method": "CREDIT_CARD", "description": "Cliente frequente (SP) - Compra periféricos e eletrônicos"},
    {"user_id": 21, "name": "Carlos Eduardo", "role": "REGULAR", "state": "RJ", "avatar": "👨‍💼", "payment_method": "PIX_INSTANT", "description": "Cliente VIP (RJ) - Usa cupons de desconto"},
    {"user_id": 34, "name": "Mariana Costa", "role": "REGULAR", "state": "MG", "avatar": "👩‍🔬", "payment_method": "CREDIT_CARD", "description": "Desenvolvedora (MG) - Montando home office"},
    {"user_id": 42, "name": "Rafael Mendes", "role": "REGULAR", "state": "PR", "avatar": "🧑‍🚀", "payment_method": "CREDIT_CARD", "description": "Gamer entusiasta (PR) - Compras recorrentes"},
    {"user_id": 55, "name": "Fernanda Lima", "role": "REGULAR", "state": "SC", "avatar": "👩‍🎨", "payment_method": "DEBIT_CARD", "description": "Designer (SC) - Compra monitores e áudio"},
    {"user_id": 68, "name": "Lucas Oliveira", "role": "REGULAR", "state": "SP", "avatar": "👨‍🔧", "payment_method": "CREDIT_CARD", "description": "Engenheiro de Dados (SP) - Compra SSDs e hardware"},
    # Comprador de Alto Volume / Anomalia Comportamental (Sem ID na blocklist, mas engatilha regra heurística do Rust!)
    {"user_id": 88, "name": " Roberto (Atacadista/Anômalo)", "role": "ANOMALY_WHALE", "state": "SP", "avatar": "🐳", "payment_method": "CORPORATE_CARD", "description": "Conta legítima, mas faz pedidos gigantes (>= R$ 50k ou >= 15 itens) que acionam o Antifraude Comportamental no Rust!"},
    # Usuários na Blocklist de Fraude (IDs 123 e 456 conforme solicitado)
    {"user_id": 123, "name": "Fraudador Blocklist #123", "role": "FRAUD_BLOCKLIST", "state": "RJ", "avatar": "🕵️‍♂️", "payment_method": "CREDIT_CARD", "description": "UserID 123 na Blocklist Histórica -> Bloqueado pelo Rust (Score 95) + Rollback de Estoque no Python"},
    {"user_id": 456, "name": "Bot Malicioso #456", "role": "FRAUD_BLOCKLIST", "state": "SP", "avatar": "🤖", "payment_method": "CREDIT_CARD", "description": "UserID 456 na Blocklist Histórica -> Bloqueado pelo Rust (Score 95) + Rollback de Estoque no Python"},
    # Usuário com Cartão Sem Saldo (ID 789 conforme solicitado)
    {"user_id": 789, "name": "João (Cartão Sem Saldo #789)", "role": "NO_FUNDS_789", "state": "MG", "avatar": "💳", "payment_method": "DECLINED_CARD", "description": "UserID 789 passa pelo Antifraude, mas tem pagamento recusado no Java (HTTP 402) -> Rollback de Estoque no Python"},
]


def ensure_dir(file_path: str) -> None:
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def seed_all_databases(reset_history: bool = False) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    summary = {}

    # 1. Product Service SQLite (products.db)
    ensure_dir(PRODUCTS_DB)
    conn = sqlite3.connect(PRODUCTS_DB, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
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
        """
    )
    for p in CATALOG_SEEDS:
        pid, sku, name, cat, price, weight, _, _ = p
        conn.execute(
            """
            INSERT INTO products (id, sku, name, category, price, weightKg, active, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(id) DO UPDATE SET
                sku=excluded.sku,
                name=excluded.name,
                category=excluded.category,
                price=excluded.price,
                weightKg=excluded.weightKg,
                active=1
            """,
            (pid, sku, name, cat, price, weight, now),
        )
    conn.commit()
    summary["products_seeded"] = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    conn.close()

    # 2. Inventory Service SQLite (inventory.db)
    ensure_dir(INVENTORY_DB)
    conn = sqlite3.connect(INVENTORY_DB, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
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
    conn.execute(
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
    if reset_history:
        conn.execute("DELETE FROM stock_reservations")

    for p in CATALOG_SEEDS:
        pid, sku, name, _, _, _, initial_stock, threshold = p
        conn.execute(
            """
            INSERT INTO stock_items (product_id, sku, product_name, available_qty, reserved_qty, reorder_threshold, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                sku=excluded.sku,
                product_name=excluded.product_name,
                available_qty=excluded.available_qty,
                reserved_qty=0,
                reorder_threshold=excluded.reorder_threshold,
                updated_at=excluded.updated_at
            """,
            (pid, sku, name, initial_stock, threshold, now),
        )
    conn.commit()
    summary["inventory_items_seeded"] = conn.execute("SELECT COUNT(*) FROM stock_items").fetchone()[0]
    conn.close()

    # 3. Pricing Service SQLite (pricing.db)
    ensure_dir(PRICING_DB)
    conn = sqlite3.connect(PRICING_DB, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS coupons (
            code TEXT PRIMARY KEY,
            discount_pct REAL NOT NULL,
            max_discount REAL NOT NULL,
            description TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tax_rules (
            state_code TEXT PRIMARY KEY,
            rate_pct REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            subtotal REAL NOT NULL,
            volume_discount REAL NOT NULL,
            coupon_discount REAL NOT NULL,
            tax_amount REAL NOT NULL,
            final_price REAL NOT NULL,
            coupon_code TEXT,
            state_code TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    if reset_history:
        conn.execute("DELETE FROM price_quotes")

    conn.execute(
        """
        INSERT OR REPLACE INTO coupons (code, discount_pct, max_discount, description) VALUES
        ('WELCOME10', 10.0, 500.0, '10% de boas-vindas'),
        ('OVERENGINEER25', 25.0, 2500.0, '25% Sandbox Poliglota Especial'),
        ('VIP50', 50.0, 5000.0, '50% Cliente VIP')
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO tax_rules (state_code, rate_pct) VALUES
        ('SP', 12.0), ('RJ', 14.0), ('MG', 10.0), ('PR', 11.0), ('SC', 9.0)
        """
    )
    conn.commit()
    conn.close()

    # 4. Simulator SQLite (simulator.db) - Perfis de usuários e log de sessões simuladas
    ensure_dir(SIMULATOR_DB)
    conn = sqlite3.connect(SIMULATOR_DB, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS virtual_users (
            user_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            state TEXT NOT NULL,
            avatar TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            description TEXT NOT NULL
        )
        """
    )
    conn.execute("DELETE FROM virtual_users")
    for u in VIRTUAL_USERS:
        conn.execute(
            """
            INSERT INTO virtual_users (user_id, name, role, state, avatar, payment_method, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (u["user_id"], u["name"], u["role"], u["state"], u["avatar"], u["payment_method"], u["description"]),
        )
    conn.commit()
    summary["virtual_users_seeded"] = len(VIRTUAL_USERS)
    conn.close()

    # 5. Se reset_history=True, limpa tabelas de histórico transacional dos demais serviços
    if reset_history:
        for db_file, tables in [
            (ORDERS_DB, ["orders", "saga_logs", "outbox_events"]),
            (FRAUD_DB, ["fraud_evaluations"]),
            (PAYMENTS_DB, ["payments", "ledger_entries"]),
            (NOTIFICATIONS_DB, ["invoices", "notification_logs"]),
            (GATEWAY_DB, ["request_logs"]),
        ]:
            if os.path.exists(db_file):
                try:
                    c = sqlite3.connect(db_file, timeout=10.0)
                    for t in tables:
                        c.execute(f"DELETE FROM {t}")
                    c.commit()
                    c.close()
                except Exception:
                    pass

    summary["reset_history"] = reset_history
    summary["timestamp"] = now
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Popula e restaura os bancos SQLite dos microsserviços.")
    parser.add_argument("--reset", action="store_true", help="Limpa o histórico de pedidos/pagamentos e restaura estoques.")
    args = parser.parse_args()
    res = seed_all_databases(reset_history=args.reset)
    print(f"[SQLite Seeder] Concluído com sucesso: {res}")
