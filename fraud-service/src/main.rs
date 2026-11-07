use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// ALGEBRAIC DATA TYPES (ADTs) & MODELAGEM FUNCIONAL EM RUST
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Approved,
    ManualReview,
    Rejected,
}

impl Verdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Verdict::Approved => "APPROVED",
            Verdict::ManualReview => "MANUAL_REVIEW",
            Verdict::Rejected => "REJECTED",
        }
    }
}

#[derive(Debug, Clone)]
pub enum RiskSignal {
    BlacklistedUser { user_id: i64, penalty: u32 },
    HighTicketAnomaly { amount: f64, penalty: u32 },
    BulkQuantityAlert { quantity: i64, penalty: u32 },
    StandardVelocity { penalty: u32 },
}

impl RiskSignal {
    pub fn score(&self) -> u32 {
        match self {
            RiskSignal::BlacklistedUser { penalty, .. } => *penalty,
            RiskSignal::HighTicketAnomaly { penalty, .. } => *penalty,
            RiskSignal::BulkQuantityAlert { penalty, .. } => *penalty,
            RiskSignal::StandardVelocity { penalty } => *penalty,
        }
    }

    pub fn label(&self) -> String {
        match self {
            RiskSignal::BlacklistedUser { user_id, .. } => {
                format!("BLOCKLIST_USER_{}", user_id)
            }
            RiskSignal::HighTicketAnomaly { amount, .. } => {
                format!("HIGH_TICKET_AMOUNT_{:.2}", amount)
            }
            RiskSignal::BulkQuantityAlert { quantity, .. } => {
                format!("BULK_QTY_{}", quantity)
            }
            RiskSignal::StandardVelocity { .. } => "NORMAL_BEHAVIOR".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FraudRequest {
    pub order_id: i64,
    pub user_id: i64,
    pub product_id: i64,
    pub quantity: i64,
    pub total_amount: f64,
}

#[derive(Debug, Clone)]
pub struct FraudDecision {
    pub order_id: i64,
    pub user_id: i64,
    pub risk_score: u32,
    pub verdict: Verdict,
    pub signals: Vec<String>,
}

// =============================================================================
// MOTOR PURO DE AVALIAÇÃO DE REGRAS (Pattern Matching + Iteradores Funcionais)
// =============================================================================

pub fn evaluate_fraud_risk(req: &FraudRequest) -> FraudDecision {
    let mut raw_signals: Vec<RiskSignal> = Vec::new();

    // Regra 1: Usuários na lista de bloqueio (Ex: userId 123, 456, 666 ou 999 para simulação da Saga)
    if req.user_id == 123 || req.user_id == 456 || req.user_id == 666 || req.user_id == 999 {
        raw_signals.push(RiskSignal::BlacklistedUser {
            user_id: req.user_id,
            penalty: 95,
        });
    }

    // Regra 2: Valor atípico da transação
    if req.total_amount >= 50000.0 {
        raw_signals.push(RiskSignal::HighTicketAnomaly {
            amount: req.total_amount,
            penalty: 85,
        });
    } else if req.total_amount >= 25000.0 {
        raw_signals.push(RiskSignal::HighTicketAnomaly {
            amount: req.total_amount,
            penalty: 40,
        });
    }

    // Regra 3: Quantidade suspeita de itens de alto valor
    if req.quantity >= 15 {
        raw_signals.push(RiskSignal::BulkQuantityAlert {
            quantity: req.quantity,
            penalty: 45,
        });
    }

    if raw_signals.is_empty() {
        raw_signals.push(RiskSignal::StandardVelocity { penalty: 5 });
    }

    // Redução funcional (fold) sobre os sinais imutáveis
    let total_score: u32 = raw_signals
        .iter()
        .map(|s| s.score())
        .fold(0u32, |acc, s| (acc + s).min(100));

    // Pattern Matching exaustivo sobre faixas de score
    let verdict = match total_score {
        80..=100 => Verdict::Rejected,
        50..=79 => Verdict::ManualReview,
        _ => Verdict::Approved,
    };

    let signals = raw_signals.iter().map(|s| s.label()).collect();

    FraudDecision {
        order_id: req.order_id,
        user_id: req.user_id,
        risk_score: total_score,
        verdict,
        signals,
    }
}

// =============================================================================
// CAMADA DE PERSISTÊNCIA SQLITE (fraud.db)
// =============================================================================

fn db_path() -> String {
    env::var("SQLITE_DB_PATH").unwrap_or_else(|_| "data/fraud.db".to_string())
}

fn exec_sqlite(sql: &str) -> String {
    let path = db_path();
    if let Some(parent) = Path::new(&path).parent() {
        let _ = fs::create_dir_all(parent);
    }

    let py_script = r#"
import sqlite3, json, sys
conn = sqlite3.connect(sys.argv[1])
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute(sys.argv[2])
if sys.argv[2].strip().upper().startswith("SELECT"):
    print(json.dumps([dict(r) for r in cur.fetchall()]))
else:
    conn.commit()
    print("[]")
conn.close()
"#;

    let output = Command::new("python")
        .arg("-c")
        .arg(py_script)
        .arg(&path)
        .arg(sql)
        .output();

    match output {
        Ok(out) => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        Err(_) => "[]".to_string(),
    }
}

fn init_db() {
    exec_sqlite(
        "CREATE TABLE IF NOT EXISTS fraud_evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            total_amount REAL NOT NULL,
            risk_score INTEGER NOT NULL,
            verdict TEXT NOT NULL,
            signals TEXT NOT NULL,
            evaluated_at INTEGER NOT NULL
        )",
    );
    println!(
        "[Fraud Service - Rust] SQLite inicializado em: {}",
        db_path()
    );
}

// =============================================================================
// PARSER JSON ZERO-COPY & SERVIDOR HTTP NATIVO EM RUST
// =============================================================================

fn extract_number(json: &str, key: &str, default: f64) -> f64 {
    let pattern = format!("\"{}\"", key);
    if let Some(pos) = json.find(&pattern) {
        let rest = &json[pos + pattern.len()..];
        if let Some(colon) = rest.find(':') {
            let after_colon = rest[colon + 1..].trim_start();
            let num_str: String = after_colon
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                .collect();
            if let Ok(v) = num_str.parse::<f64>() {
                return v;
            }
        }
    }
    default
}

fn send_http_json(mut stream: TcpStream, status: u16, status_text: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        status_text,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0u8; 8192];
    let bytes_read = match stream.read(&mut buffer) {
        Ok(0) | Err(_) => return,
        Ok(n) => n,
    };

    let raw_req = String::from_utf8_lossy(&buffer[..bytes_read]);
    let first_line = raw_req.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        return;
    }

    let method = parts[0];
    let path = parts[1];
    let body = raw_req.split("\r\n\r\n").nth(1).unwrap_or("");

    match (method, path) {
        ("GET", "/health") => {
            let json = format!(
                "{{\"service\":\"fraud-service\",\"language\":\"Rust (Native Memory-Safe / Zero-GC)\",\"status\":\"UP\",\"database\":\"{}\"}}",
                db_path()
            );
            send_http_json(stream, 200, "OK", &json);
        }
        ("GET", "/fraud/history") => {
            let rows = exec_sqlite("SELECT * FROM fraud_evaluations ORDER BY id DESC LIMIT 50");
            send_http_json(stream, 200, "OK", &rows);
        }
        ("POST", "/fraud/evaluate") => {
            let req = FraudRequest {
                order_id: extract_number(body, "orderId", 0.0) as i64,
                user_id: extract_number(body, "userId", 0.0) as i64,
                product_id: extract_number(body, "productId", 0.0) as i64,
                quantity: extract_number(body, "quantity", 1.0) as i64,
                total_amount: extract_number(body, "totalAmount", 0.0),
            };

            let decision = evaluate_fraud_risk(&req);
            let now_epoch = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let signals_json = format!(
                "[{}]",
                decision
                    .signals
                    .iter()
                    .map(|s| format!("\"{}\"", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );

            let insert_sql = format!(
                "INSERT INTO fraud_evaluations (order_id, user_id, total_amount, risk_score, verdict, signals, evaluated_at) VALUES ({}, {}, {:.2}, {}, '{}', '{}', {})",
                decision.order_id,
                decision.user_id,
                req.total_amount,
                decision.risk_score,
                decision.verdict.as_str(),
                signals_json.replace('\'', "''"),
                now_epoch
            );
            exec_sqlite(&insert_sql);

            println!(
                "[Fraud Service - Rust] Order #{} (User {}) -> Score: {} | Verdict: {}",
                decision.order_id,
                decision.user_id,
                decision.risk_score,
                decision.verdict.as_str()
            );

            let resp_json = format!(
                "{{\"service\":\"fraud-service\",\"language\":\"Rust\",\"orderId\":{},\"userId\":{},\"riskScore\":{},\"verdict\":\"{}\",\"signals\":{}}}",
                decision.order_id,
                decision.user_id,
                decision.risk_score,
                decision.verdict.as_str(),
                signals_json
            );
            send_http_json(stream, 200, "OK", &resp_json);
        }
        _ => {
            send_http_json(
                stream,
                404,
                "Not Found",
                "{\"message\":\"Rota não encontrada no fraud-service (Rust)\"}",
            );
        }
    }
}

fn main() {
    init_db();
    let port = env::var("PORT").unwrap_or_else(|_| "3005".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).expect("Falha ao abrir porta no Fraud Service (Rust)");

    println!(
        "[Fraud Service - Rust] Rodando na porta {} | SQLite: {}",
        port,
        db_path()
    );

    for stream in listener.incoming().flatten() {
        thread::spawn(move || {
            handle_client(stream);
        });
    }
}
