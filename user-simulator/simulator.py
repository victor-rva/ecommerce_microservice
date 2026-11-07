"""
Painel Gerencial Web + Simulador Visual no Terminal para a Arquitetura de Microsserviços Poliglota.
Simula diversos usuários simultâneos acessando o API Gateway (:3000) e atravessando os 7 microsserviços downstream,
preenchendo e monitorando os 8 bancos SQLite em tempo real.

- Interface Gerencial Web: http://localhost:3080
- Dashboard Live no Terminal: exibido automaticamente ao rodar
"""
import json
import os
import random
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from seeder import (
    CATALOG_SEEDS,
    FRAUD_DB,
    GATEWAY_DB,
    INVENTORY_DB,
    NOTIFICATIONS_DB,
    ORDERS_DB,
    PAYMENTS_DB,
    PRICING_DB,
    PRODUCTS_DB,
    SIMULATOR_DB,
    VIRTUAL_USERS,
    seed_all_databases,
)

PORT = int(os.environ.get("PORT", "3080"))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:3000")
AUTO_START = os.environ.get("AUTO_START", "true").lower() == "true"

# Estado compartilhado em memória entre as threads de usuários virtuais, o Terminal e o Painel Web
STATE_LOCK = threading.Lock()
SIM_STATE = {
    "running": AUTO_START,
    "concurrency": 5,           # Número de bots ativos simultaneamente
    "interval_sec": 2.2,        # Cadência entre ações de cada usuário
    "started_at": datetime.now().isoformat(),
    "metrics": {
        "total_requests": 0,
        "approved_orders": 0,
        "total_revenue": 0.0,
        "fraud_blocklist_rejected": 0,   # IDs 123, 456
        "fraud_behavior_rejected": 0,    # Ticket >= 50k ou >= 25k + Qty >= 15
        "payment_declined_789": 0,       # ID 789 / DECLINED_CARD
        "out_of_stock_409": 0,           # Produto esgotado no Python
        "saga_rollbacks": 0,             # Total de compensações executadas no estoque
        "avg_latency_ms": 0,
    },
    "latencies": deque(maxlen=100),
    "user_states": {},          # user_id -> estado atual em tempo real
    "recent_events": deque(maxlen=40),
}

for u in VIRTUAL_USERS:
    SIM_STATE["user_states"][u["user_id"]] = {
        **u,
        "current_action": "Aguardando início da sessão...",
        "current_step": "IDLE",
        "last_order_id": None,
        "last_status": "IDLE",
        "orders_count": 0,
        "updated_at": datetime.now().strftime("%H:%M:%S"),
    }


def http_json(method: str, url: str, payload: dict | None = None, timeout: float = 8.0) -> tuple[int, dict | list]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "identity",
    }
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8")
            return e.code, json.loads(raw) if raw else {"message": str(e)}
        except Exception:
            return e.code, {"message": str(e)}
    except Exception as ex:
        return 503, {"message": str(ex)}


def record_event(kind: str, badge: str, user: dict, title: str, detail: str, order_id: int | None = None, amount: float = 0.0, latency_ms: int = 0) -> None:
    with STATE_LOCK:
        SIM_STATE["recent_events"].appendleft({
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "kind": kind,        # SUCCESS, FRAUD_ID, FRAUD_BEHAVIOR, PAYMENT_DECLINED, OUT_OF_STOCK, INFO
            "badge": badge,
            "userId": user["user_id"],
            "userName": user["name"],
            "avatar": user["avatar"],
            "role": user["role"],
            "orderId": order_id,
            "title": title,
            "detail": detail,
            "amount": amount,
            "latencyMs": latency_ms,
        })


def update_user_activity(user_id: int, step: str, action: str, last_status: str | None = None, order_id: int | None = None, inc_orders: bool = False) -> None:
    with STATE_LOCK:
        st = SIM_STATE["user_states"].get(user_id)
        if not st:
            return
        st["current_step"] = step
        st["current_action"] = action
        st["updated_at"] = datetime.now().strftime("%H:%M:%S")
        if last_status:
            st["last_status"] = last_status
        if order_id:
            st["last_order_id"] = order_id
        if inc_orders:
            st["orders_count"] += 1


def execute_simulated_order(user: dict, custom_product_id: int | None = None, custom_qty: int | None = None, custom_coupon: str | None = None) -> dict:
    uid = user["user_id"]
    role = user["role"]

    # Fase 1: Usuário navega no catálogo via API Gateway
    update_user_activity(uid, "BROWSING_CATALOG", "Consultando catálogo em /api/products (Go -> TypeScript :3001)...")
    time.sleep(0.25)

    # Escolha inteligente do produto e quantidade conforme o perfil do usuário
    if custom_product_id is not None:
        product_id = custom_product_id
    elif role == "ANOMALY_WHALE":
        # Escolhe Workstation GPU (105 - R$ 28.000) ou NAS (110 - R$ 9.800) ou Laptop (101 - R$ 6.500)
        product_id = random.choice([105, 110, 101])
    elif role in ("FRAUD_BLOCKLIST", "NO_FUNDS_789"):
        product_id = random.choice([101, 103, 104, 106, 107, 108])
    else:
        product_id = random.choice([p[0] for p in CATALOG_SEEDS])

    if custom_qty is not None:
        quantity = custom_qty
    elif role == "ANOMALY_WHALE":
        # Quantidade alta para disparar regra heurística comportamental no Rust (Ticket >= 50k ou >= 25k + Qty >= 15)
        quantity = 2 if product_id == 105 else random.choice([8, 15, 16])
    elif role == "FRAUD_BLOCKLIST":
        quantity = random.randint(1, 3)
    elif role == "NO_FUNDS_789":
        quantity = random.randint(1, 2)
    else:
        quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]

    coupon = custom_coupon if custom_coupon is not None else random.choice([None, None, "WELCOME10", "OVERENGINEER25", "VIP50"])

    prod_name = next((p[2] for p in CATALOG_SEEDS if p[0] == product_id), f"Produto #{product_id}")
    update_user_activity(
        uid,
        "SAGA_IN_PROGRESS",
        f"Comprando {quantity}x {prod_name} ( atravessando 7 microsserviços )...",
    )

    order_payload = {
        "userId": uid,
        "productId": product_id,
        "quantity": quantity,
        "couponCode": coupon,
        "state": user["state"],
        "paymentMethod": user["payment_method"],
    }

    start_t = time.time()
    status_code, resp = http_json("POST", f"{GATEWAY_URL}/api/orders", order_payload)
    latency_ms = int((time.time() - start_t) * 1000)

    order_id = resp.get("orderId") if isinstance(resp, dict) else None

    with STATE_LOCK:
        SIM_STATE["metrics"]["total_requests"] += 1
        SIM_STATE["latencies"].append(latency_ms)
        SIM_STATE["metrics"]["avg_latency_ms"] = int(sum(SIM_STATE["latencies"]) / len(SIM_STATE["latencies"]))

    # Tratamento dos cenários retornados pela Saga
    if status_code == 201 and isinstance(resp, dict):
        total_price = float(resp.get("pricing", {}).get("totalPrice", 0.0))
        inv_num = (resp.get("invoice") or {}).get("invoiceNumber", "NFE-OK")
        with STATE_LOCK:
            SIM_STATE["metrics"]["approved_orders"] += 1
            SIM_STATE["metrics"]["total_revenue"] = round(SIM_STATE["metrics"]["total_revenue"] + total_price, 2)

        update_user_activity(
            uid,
            "COMPLETED",
            f"✅ Pedido #{order_id} Aprovado! {quantity}x {prod_name} (R$ {total_price:,.2f}) | {inv_num}",
            last_status="CONFIRMED",
            order_id=order_id,
            inc_orders=True,
        )
        record_event(
            kind="SUCCESS",
            badge="✅ SAGA COMPLETA (7/7)",
            user=user,
            title=f"Pedido #{order_id} Confirmado — {quantity}x {prod_name}",
            detail=f"Clojure precificou R$ {total_price:,.2f} -> Python baixou estoque -> Rust aprovou -> Java debitou Ledger -> Go emitiu {inv_num}",
            order_id=order_id,
            amount=total_price,
            latency_ms=latency_ms,
        )

    elif status_code == 422 and isinstance(resp, dict):
        # Reprovado pelo Antifraude em Rust -> Estoque revertido no Python!
        fraud_info = resp.get("fraudAnalysis", {})
        signals = fraud_info.get("signals", [])
        score = fraud_info.get("riskScore", 95)
        is_blocklist = any("BLOCKLIST_USER" in s for s in signals)

        with STATE_LOCK:
            SIM_STATE["metrics"]["saga_rollbacks"] += 1
            if is_blocklist:
                SIM_STATE["metrics"]["fraud_blocklist_rejected"] += 1
            else:
                SIM_STATE["metrics"]["fraud_behavior_rejected"] += 1

        if is_blocklist:
            reason_label = f"🛡️ BLOQUEIO BLOCKLIST (ID #{uid})"
            detail_msg = f"Rust detectou userId={uid} na Blocklist (Score: {score}, Sinais: {', '.join(signals)}) -> Python executou ROLLBACK da reserva de {quantity}x {prod_name}!"
        else:
            reason_label = "🚨 FRAUDE COMPORTAMENTAL (Heurística)"
            detail_msg = f"Usuário sem histórico sujo, mas Rust barrou por anomalia de valor/volume (Score: {score}, Sinais: {', '.join(signals)}) -> Python estornou {quantity}x {prod_name}!"

        update_user_activity(
            uid,
            "COMPENSATED_FRAUD",
            f"{reason_label} no Pedido #{order_id} (Score {score}) -> Estoque devolvido!",
            last_status="CANCELLED_FRAUD",
            order_id=order_id,
            inc_orders=True,
        )
        record_event(
            kind="FRAUD_ID" if is_blocklist else "FRAUD_BEHAVIOR",
            badge=reason_label,
            user=user,
            title=f"Pedido #{order_id} Barrado pelo Rust Antifraude ({quantity}x {prod_name})",
            detail=detail_msg,
            order_id=order_id,
            latency_ms=latency_ms,
        )

    elif status_code == 402 and isinstance(resp, dict):
        # Recusado pelo Pagamento em Java 17 (ex: userId 789) -> Estoque revertido no Python!
        with STATE_LOCK:
            SIM_STATE["metrics"]["payment_declined_789"] += 1
            SIM_STATE["metrics"]["saga_rollbacks"] += 1

        update_user_activity(
            uid,
            "COMPENSATED_PAYMENT",
            f"💳 Cartão Sem Saldo no Java (User #{uid}, Pedido #{order_id}) -> Estoque estornado no Python!",
            last_status="PAYMENT_DECLINED",
            order_id=order_id,
            inc_orders=True,
        )
        record_event(
            kind="PAYMENT_DECLINED",
            badge=f"💳 CARTÃO RECUSADO (ID #{uid})",
            user=user,
            title=f"Pedido #{order_id} Recusado no Java 17 ({quantity}x {prod_name})",
            detail=f"Passou pelo Rust Antifraude, mas Java recusou cobrança (HTTP 402 para User #{uid}) -> Saga acionou /inventory/release no Python!",
            order_id=order_id,
            latency_ms=latency_ms,
        )

    elif status_code == 409 and isinstance(resp, dict):
        # Estoque insuficiente no Python
        with STATE_LOCK:
            SIM_STATE["metrics"]["out_of_stock_409"] += 1

        inv_err = resp.get("inventoryError", {})
        avail = inv_err.get("availableQty", 0)
        update_user_activity(
            uid,
            "OUT_OF_STOCK",
            f"📦 Sem estoque de {prod_name} (Pediu {quantity}, disp: {avail})",
            last_status="OUT_OF_STOCK",
            order_id=order_id,
        )
        record_event(
            kind="OUT_OF_STOCK",
            badge="📦 ESTOQUE ESGOTADO",
            user=user,
            title=f"Estoque Insuficiente para {prod_name} (Pedido #{order_id})",
            detail=f"Python impediu overselling: {user['name']} tentou comprar {quantity} un., mas só havia {avail} disponível.",
            order_id=order_id,
            latency_ms=latency_ms,
        )
    else:
        err_msg = resp.get("message", f"HTTP {status_code}") if isinstance(resp, dict) else str(resp)
        update_user_activity(uid, "ERROR", f"⚠️ Aguardando microsserviços ({err_msg})", last_status="ERROR", order_id=order_id)
        if order_id:
            record_event(
                kind="OUT_OF_STOCK",
                badge=f"⚠️ FALHA / COMPENSAÇÃO (HTTP {status_code})",
                user=user,
                title=f"Pedido #{order_id} interrompido ({quantity}x {prod_name})",
                detail=f"Motivo: {err_msg}",
                order_id=order_id,
                latency_ms=latency_ms,
            )

    return {"statusCode": status_code, "response": resp, "latencyMs": latency_ms}


def worker_loop(slot_index: int) -> None:
    """Loop contínuo de simulação de tráfego concorrente."""
    while True:
        with STATE_LOCK:
            running = SIM_STATE["running"]
            concurrency = SIM_STATE["concurrency"]
            interval = SIM_STATE["interval_sec"]

        if not running or slot_index >= concurrency:
            time.sleep(0.5)
            continue

        # Sorteia um usuário virtual dando peso para todos os cenários aparecerem bem
        user = random.choices(
            VIRTUAL_USERS,
            weights=[16, 15, 14, 14, 12, 12, 8, 9, 8, 10],
            k=1,
        )[0]

        try:
            execute_simulated_order(user)
        except Exception:
            pass

        # Jitter natural de navegação humana
        sleep_time = max(0.4, interval + random.uniform(-0.4, 0.7))
        time.sleep(sleep_time)


def query_sqlite_safe(db_path: str, sql: str, params: tuple = ()) -> list[dict]:
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path, timeout=3.0)
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def scalar_sqlite_safe(db_path: str, sql: str) -> int:
    rows = query_sqlite_safe(db_path, sql)
    if not rows:
        return 0
    val = list(rows[0].values())[0]
    return int(val or 0)


def hydrate_recent_events_from_orders(recent_orders: list[dict]) -> None:
    """Hidrata o Feed da Saga a partir dos pedidos já gravados em orders.db caso a lista em memória esteja vazia."""
    if not recent_orders:
        return
    users_by_id = {u["user_id"]: u for u in VIRTUAL_USERS}
    default_user = VIRTUAL_USERS[0]

    with STATE_LOCK:
        if len(SIM_STATE["recent_events"]) > 0:
            return
        for o in reversed(recent_orders[:20]):
            oid = o.get("orderId")
            uid = int(o.get("userId") or 10)
            user = users_by_id.get(uid, {**default_user, "user_id": uid, "name": f"Cliente #{uid}"})
            status = str(o.get("status") or "")
            qty = int(o.get("quantity") or 1)
            pname = o.get("productName") or f"Produto #{o.get('productId')}"
            total = float(o.get("totalPrice") or 0.0)
            score = o.get("fraudScore") or 95
            created_raw = str(o.get("createdAt") or "")
            ts = created_raw[11:19] if len(created_raw) >= 19 else datetime.now().strftime("%H:%M:%S")

            if status == "CONFIRMED":
                kind = "SUCCESS"
                badge = "✅ SAGA COMPLETA (7/7)"
                title = f"Pedido #{oid} Confirmado — {qty}x {pname}"
                detail = f"Clojure precificou R$ {total:,.2f} -> Python baixou estoque -> Rust aprovou -> Java debitou Ledger -> Go emitiu NF-e"
            elif status == "CANCELLED_FRAUD_DETECTED":
                is_bl = uid in (123, 456, 666, 999)
                kind = "FRAUD_ID" if is_bl else "FRAUD_BEHAVIOR"
                badge = f"🛡️ BLOQUEIO BLOCKLIST (ID #{uid})" if is_bl else "🚨 FRAUDE COMPORTAMENTAL (Heurística)"
                title = f"Pedido #{oid} Barrado pelo Rust Antifraude ({qty}x {pname})"
                detail = f"Rust reprovou (Score: {score}) -> Python executou ROLLBACK da reserva de {qty}x {pname}!"
            elif status == "CANCELLED_PAYMENT_DECLINED":
                kind = "PAYMENT_DECLINED"
                badge = f"💳 CARTÃO RECUSADO (ID #{uid})"
                title = f"Pedido #{oid} Recusado no Java 17 ({qty}x {pname})"
                detail = f"Passou pelo Antifraude, mas Java recusou pagamento (HTTP 402) -> Python estornou reserva de estoque!"
            elif status == "REJECTED_OUT_OF_STOCK":
                kind = "OUT_OF_STOCK"
                badge = "📦 ESTOQUE ESGOTADO"
                title = f"Estoque Insuficiente para {pname} (Pedido #{oid})"
                detail = f"Python bloqueou reserva de {qty}x {pname} por falta de saldo disponível."
            else:
                kind = "OUT_OF_STOCK"
                badge = f"🔄 SAGA COMPENSADA ({status})"
                title = f"Pedido #{oid} — {qty}x {pname}"
                detail = f"Status registrado em orders.db: {status} (Clique para inspecionar os passos executados)"

            SIM_STATE["recent_events"].appendleft({
                "timestamp": ts,
                "kind": kind,
                "badge": badge,
                "userId": user["user_id"],
                "userName": user["name"],
                "avatar": user["avatar"],
                "role": user["role"],
                "orderId": oid,
                "title": title,
                "detail": detail,
                "amount": total,
                "latencyMs": 45,
            })


def get_order_trace_fast(order_id_str: str) -> tuple[int, dict]:
    """Busca a trilha completa da Saga diretamente em orders.db (instantâneo) ou via API do order-service."""
    try:
        oid = int(order_id_str)
    except ValueError:
        return 400, {"message": f"ID de pedido inválido: {order_id_str}"}

    # 1. Consulta direta ao SQLite orders.db (0-2ms, imune a timeout de rede)
    orders = query_sqlite_safe(ORDERS_DB, "SELECT * FROM orders WHERE orderId = ?", (oid,))
    saga_rows = query_sqlite_safe(
        ORDERS_DB,
        "SELECT * FROM saga_logs WHERE orderId = ? ORDER BY stepOrder ASC, id ASC",
        (oid,),
    )
    outbox_rows = query_sqlite_safe(
        ORDERS_DB,
        "SELECT * FROM outbox_events WHERE aggregateId = ? ORDER BY id ASC",
        (oid,),
    )

    if orders and saga_rows:
        parsed_steps = []
        for s in saga_rows:
            raw_details = s.get("details") or "{}"
            try:
                details_obj = json.loads(raw_details) if isinstance(raw_details, str) else raw_details
            except Exception:
                details_obj = {"raw": str(raw_details)}
            parsed_steps.append({**s, "details": details_obj})

        return 200, {
            "order": orders[0],
            "sagaExecutionTrace": parsed_steps,
            "outboxEvents": outbox_rows,
        }

    # 2. Fallback HTTP rápido direto ao order-service (:3002) ou Gateway (:3000)
    code, data = http_json("GET", f"{GATEWAY_URL}/api/orders/{oid}", timeout=2.5)
    if code == 200 and isinstance(data, dict) and data.get("sagaExecutionTrace"):
        return 200, data

    if orders:
        return 200, {
            "order": orders[0],
            "sagaExecutionTrace": saga_rows,
            "outboxEvents": outbox_rows,
        }

    return 404, {"message": f"Trilha da Saga para o Pedido #{oid} não encontrada."}


def collect_live_dashboard_snapshot() -> dict:
    """Coleta dados ao vivo dos 8 bancos SQLite e do estado da simulação."""
    inventory_items = query_sqlite_safe(
        INVENTORY_DB,
        "SELECT product_id, sku, product_name, available_qty, reserved_qty, reorder_threshold, updated_at FROM stock_items ORDER BY product_id ASC",
    )
    recent_orders = query_sqlite_safe(
        ORDERS_DB,
        "SELECT orderId, correlationId, userId, productId, productName, quantity, totalPrice, couponCode, stateCode, fraudVerdict, fraudScore, status, createdAt FROM orders ORDER BY createdAt DESC LIMIT 18",
    )
    hydrate_recent_events_from_orders(recent_orders)
    recent_fraud = query_sqlite_safe(
        FRAUD_DB,
        "SELECT order_id, user_id, total_amount, risk_score, verdict, signals, evaluated_at FROM fraud_evaluations ORDER BY id DESC LIMIT 12",
    )
    recent_payments = query_sqlite_safe(
        PAYMENTS_DB,
        "SELECT transaction_id, order_id, user_id, amount, payment_method, status, created_at FROM payments ORDER BY id DESC LIMIT 12",
    )
    recent_reservations = query_sqlite_safe(
        INVENTORY_DB,
        "SELECT reservation_id, order_id, product_id, quantity, status, reason, updated_at FROM stock_reservations ORDER BY updated_at DESC LIMIT 12",
    )

    sqlite_stats = [
        {"service": "api-gateway", "lang": "Go", "port": 3000, "db": "gateway.db", "table": "request_logs", "rows": scalar_sqlite_safe(GATEWAY_DB, "SELECT COUNT(*) FROM request_logs")},
        {"service": "product-service", "lang": "TypeScript", "port": 3001, "db": "products.db", "table": "products", "rows": scalar_sqlite_safe(PRODUCTS_DB, "SELECT COUNT(*) FROM products")},
        {"service": "order-service", "lang": "TypeScript", "port": 3002, "db": "orders.db", "table": "orders / saga_logs", "rows": scalar_sqlite_safe(ORDERS_DB, "SELECT COUNT(*) FROM orders"), "extra": scalar_sqlite_safe(ORDERS_DB, "SELECT COUNT(*) FROM saga_logs")},
        {"service": "pricing-service", "lang": "Clojure", "port": 3003, "db": "pricing.db", "table": "price_quotes", "rows": scalar_sqlite_safe(PRICING_DB, "SELECT COUNT(*) FROM price_quotes")},
        {"service": "inventory-service", "lang": "Python", "port": 3004, "db": "inventory.db", "table": "stock_reservations", "rows": scalar_sqlite_safe(INVENTORY_DB, "SELECT COUNT(*) FROM stock_reservations")},
        {"service": "fraud-service", "lang": "Rust", "port": 3005, "db": "fraud.db", "table": "fraud_evaluations", "rows": scalar_sqlite_safe(FRAUD_DB, "SELECT COUNT(*) FROM fraud_evaluations")},
        {"service": "payment-service", "lang": "Java 17", "port": 3006, "db": "payments.db", "table": "payments / ledger", "rows": scalar_sqlite_safe(PAYMENTS_DB, "SELECT COUNT(*) FROM payments"), "extra": scalar_sqlite_safe(PAYMENTS_DB, "SELECT COUNT(*) FROM ledger_entries")},
        {"service": "notification-service", "lang": "Go", "port": 3007, "db": "notifications.db", "table": "invoices (NF-e)", "rows": scalar_sqlite_safe(NOTIFICATIONS_DB, "SELECT COUNT(*) FROM invoices")},
    ]

    with STATE_LOCK:
        return {
            "running": SIM_STATE["running"],
            "concurrency": SIM_STATE["concurrency"],
            "intervalSec": SIM_STATE["interval_sec"],
            "metrics": dict(SIM_STATE["metrics"]),
            "virtualUsers": list(SIM_STATE["user_states"].values()),
            "recentEvents": list(SIM_STATE["recent_events"]),
            "inventory": inventory_items,
            "recentOrders": recent_orders,
            "recentFraud": recent_fraud,
            "recentPayments": recent_payments,
            "recentReservations": recent_reservations,
            "sqliteDatabases": sqlite_stats,
            "serverTime": datetime.now().strftime("%H:%M:%S"),
        }


def terminal_renderer_loop() -> None:
    """Exibe um resumo visual colorido no terminal enquanto o painel web também está ativo."""
    if os.name == "nt":
        os.system("")  # Habilita sequências ANSI no Windows 10/11
    while True:
        time.sleep(2.0)
        snap = collect_live_dashboard_snapshot()
        m = snap["metrics"]
        status_badge = "\033[92m● RODANDO\033[0m" if snap["running"] else "\033[93m⏸ PAUSADO\033[0m"

        lines = [
            "\033[2J\033[H",
            "\033[96m========================================================================================================\033[0m",
            f" 🌐 \033[1mPAINEL GERENCIAL & SIMULADOR DE USUÁRIOS\033[0m | Status: {status_badge} | Bots: {snap['concurrency']} | Web UI: \033[93mhttp://localhost:{PORT}\033[0m",
            "\033[96m========================================================================================================\033[0m",
            f" 📊 \033[1mMÉTRICAS AO VIVO\033[0m: ✅ Aprovados: \033[92m{m['approved_orders']}\033[0m (R$ {m['total_revenue']:,.2f}) | 🛡️ Fraude ID (123/456): \033[91m{m['fraud_blocklist_rejected']}\033[0m | 🚨 Fraude Valor/Vol: \033[95m{m['fraud_behavior_rejected']}\033[0m",
            f"    💳 Cartão S/ Saldo (789): \033[93m{m['payment_declined_789']}\033[0m | 📦 Sem Estoque: \033[90m{m['out_of_stock_409']}\033[0m | 🔄 Rollbacks Saga: \033[96m{m['saga_rollbacks']}\033[0m | ⚡ Latência: {m['avg_latency_ms']}ms",
            "\033[90m--------------------------------------------------------------------------------------------------------\033[0m",
            " 👥 \033[1mUSUÁRIOS SIMULADOS (Tempo Real)\033[0m:",
        ]

        for u in snap["virtualUsers"][:10]:
            role_color = "\033[92m"
            if u["role"] == "FRAUD_BLOCKLIST":
                role_color = "\033[91m"
            elif u["role"] == "ANOMALY_WHALE":
                role_color = "\033[95m"
            elif u["role"] == "NO_FUNDS_789":
                role_color = "\033[93m"
            lines.append(
                f"  {u['avatar']} {role_color}[ID {u['user_id']:<3}] {u['name']:<28}\033[0m -> {u['current_action'][:62]}"
            )

        lines.append("\033[90m--------------------------------------------------------------------------------------------------------\033[0m")
        lines.append(" 📦 \033[1mSALDO DOS BANCOS SQLITE (inventory.db & 8 Microsserviços)\033[0m:")
        inv_line = []
        for item in snap["inventory"][:6]:
            av = item["available_qty"]
            col = "\033[92m" if av > item["reorder_threshold"] else ("\033[93m" if av > 0 else "\033[91m")
            inv_line.append(f"[{item['product_id']}] {item['product_name'][:16]}: {col}{av} disp\033[0m")
        lines.append("  " + " | ".join(inv_line))

        db_line = [f"{d['db']}: \033[96m{d['rows']}\033[0m regs" for d in snap["sqliteDatabases"]]
        lines.append("  🗄️  " + " | ".join(db_line[:4]))
        lines.append("  🗄️  " + " | ".join(db_line[4:]))

        lines.append("\033[90m--------------------------------------------------------------------------------------------------------\033[0m")
        lines.append(" 📜 \033[1mÚLTIMOS EVENTOS DA SAGA\033[0m:")
        for ev in snap["recentEvents"][:5]:
            lines.append(f"  [{ev['timestamp']}] {ev['badge']} \033[1m{ev['userName']}\033[0m: {ev['title']}")

        lines.append("\033[96m========================================================================================================\033[0m")
        lines.append(f" 💡 Abra o Painel Gerencial Interativo no navegador: \033[1;93mhttp://localhost:{PORT}\033[0m (Pressione Ctrl+C para sair)")
        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()


DASHBOARD_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")


class SimulatorHTTPHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: dict | list) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
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

        if path == "/":
            with open(DASHBOARD_HTML_PATH, "rb") as f:
                html = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return

        if path == "/api/state":
            self._send_json(200, collect_live_dashboard_snapshot())
            return

        if path.startswith("/api/order-trace/"):
            order_id = path.split("/")[-1]
            code, data = get_order_trace_fast(order_id)
            self._send_json(code, data)
            return

        self._send_json(404, {"message": "Not Found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        body = self._read_json()

        if path == "/api/control/toggle":
            with STATE_LOCK:
                if "running" in body:
                    SIM_STATE["running"] = bool(body["running"])
                else:
                    SIM_STATE["running"] = not SIM_STATE["running"]
                if "concurrency" in body:
                    SIM_STATE["concurrency"] = max(1, min(10, int(body["concurrency"])))
                if "intervalSec" in body:
                    SIM_STATE["interval_sec"] = max(0.5, min(10.0, float(body["intervalSec"])))
                running = SIM_STATE["running"]
            self._send_json(200, {"running": running})
            return

        if path == "/api/control/seed":
            reset = bool(body.get("reset", False))
            res = seed_all_databases(reset_history=reset)
            if reset:
                with STATE_LOCK:
                    for k in SIM_STATE["metrics"]:
                        SIM_STATE["metrics"][k] = 0 if isinstance(SIM_STATE["metrics"][k], int) else 0.0
                    SIM_STATE["recent_events"].clear()
            self._send_json(200, {"seeded": True, "details": res})
            return

        if path == "/api/control/trigger":
            # Dispara manualmente um cenário específico escolhido pelo usuário no Painel Gerencial!
            scenario = body.get("scenario", "HAPPY")
            if scenario == "FRAUD_123":
                user = next(u for u in VIRTUAL_USERS if u["user_id"] == 123)
                res = execute_simulated_order(user, custom_product_id=101, custom_qty=2)
            elif scenario == "FRAUD_456":
                user = next(u for u in VIRTUAL_USERS if u["user_id"] == 456)
                res = execute_simulated_order(user, custom_product_id=104, custom_qty=1)
            elif scenario == "FRAUD_BEHAVIOR":
                # Usuário comum (ID 88), mas comprando 2x Workstation GPU (R$ 56.000) -> Pego pela Heurística!
                user = next(u for u in VIRTUAL_USERS if u["user_id"] == 88)
                res = execute_simulated_order(user, custom_product_id=105, custom_qty=2)
            elif scenario == "DECLINED_789":
                user = next(u for u in VIRTUAL_USERS if u["user_id"] == 789)
                res = execute_simulated_order(user, custom_product_id=103, custom_qty=2)
            elif scenario == "OUT_OF_STOCK":
                user = next(u for u in VIRTUAL_USERS if u["user_id"] == 10)
                res = execute_simulated_order(user, custom_product_id=112, custom_qty=50)
            else:
                user = next(u for u in VIRTUAL_USERS if u["user_id"] == 10)
                res = execute_simulated_order(user, custom_product_id=102, custom_qty=2, custom_coupon="OVERENGINEER25")

            self._send_json(200, res)
            return

        self._send_json(404, {"message": "Not Found"})


if __name__ == "__main__":
    print("[User Simulator] Populando bancos de dados SQLite iniciais (Seeder)...")
    seed_all_databases(reset_history=False)

    # Inicia threads dos usuários virtuais concorrentes
    for idx in range(10):
        t = threading.Thread(target=worker_loop, args=(idx,), daemon=True)
        t.start()

    # Inicia thread do dashboard visual no terminal
    if sys.stdout.isatty():
        t_term = threading.Thread(target=terminal_renderer_loop, daemon=True)
        t_term.start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), SimulatorHTTPHandler)
    print(f"[User Simulator & Painel Gerencial] Rodando em http://localhost:{PORT}")
    server.serve_forever()
