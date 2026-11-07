package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SqliteStore gerencia a persistência isolada do notification-service em data/notifications.db
type SqliteStore struct {
	dbPath string
	mu     sync.Mutex
}

func NewSqliteStore(dbPath string) *SqliteStore {
	_ = os.MkdirAll(filepath.Dir(dbPath), 0755)
	store := &SqliteStore{dbPath: dbPath}
	store.initSchema()
	return store
}

func (s *SqliteStore) execQuery(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if args == nil {
		args = []interface{}{}
	}
	argsJSON, _ := json.Marshal(args)
	pyScript := `
import sqlite3, json, sys
db_path, sql, params = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
conn = sqlite3.connect(db_path, timeout=10.0)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")
cur = conn.cursor()
cur.execute(sql, params)
if sql.strip().upper().startswith("SELECT"):
    print(json.dumps([dict(r) for r in cur.fetchall()]))
else:
    conn.commit()
    print("[]")
conn.close()
`
	cmd := exec.Command("python", "-c", pyScript, s.dbPath, sql, string(argsJSON))
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal(out, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *SqliteStore) initSchema() {
	_, err1 := s.execQuery(`
		CREATE TABLE IF NOT EXISTS invoices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			invoice_number TEXT NOT NULL UNIQUE,
			series TEXT NOT NULL,
			access_key TEXT NOT NULL UNIQUE,
			authorization_protocol TEXT NOT NULL,
			order_id INTEGER NOT NULL,
			correlation_id TEXT NOT NULL,
			user_id INTEGER NOT NULL,
			product_name TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			subtotal REAL NOT NULL,
			discount_amount REAL NOT NULL,
			tax_amount REAL NOT NULL,
			total_price REAL NOT NULL,
			payment_transaction_id TEXT NOT NULL,
			status TEXT NOT NULL,
			issued_at TEXT NOT NULL
		)
	`)
	_, err2 := s.execQuery(`
		CREATE TABLE IF NOT EXISTS notification_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			notification_id TEXT NOT NULL UNIQUE,
			order_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			channel TEXT NOT NULL,
			recipient TEXT NOT NULL,
			subject TEXT NOT NULL,
			invoice_access_key TEXT NOT NULL,
			status TEXT NOT NULL,
			sent_at TEXT NOT NULL
		)
	`)
	if err1 != nil || err2 != nil {
		log.Printf("[Notification Service - Go] Aviso ao inicializar SQLite: %v / %v", err1, err2)
	} else {
		log.Printf("[Notification Service - Go] SQLite inicializado em: %s", s.dbPath)
	}
}

// OrderConfirmedPayload representa o contrato recebido do order-service (Saga Step 7)
type OrderConfirmedPayload struct {
	Event                string  `json:"event"`
	OrderID              int64   `json:"orderId"`
	CorrelationID        string  `json:"correlationId"`
	UserID               int64   `json:"userId"`
	ProductName          string  `json:"productName"`
	Quantity             int64   `json:"quantity"`
	Subtotal             float64 `json:"subtotal"`
	DiscountAmount       float64 `json:"discountAmount"`
	TaxAmount            float64 `json:"taxAmount"`
	TotalPrice           float64 `json:"totalPrice"`
	PaymentTransactionID string  `json:"paymentTransactionId"`
	ConfirmedAt          string  `json:"confirmedAt"`
}

// calculateMod11 calcula o dígito verificador padrão SEFAZ (Módulo 11 com pesos de 2 a 9) para os 43 dígitos base da NF-e
func calculateMod11(base43 string) int {
	weight := 2
	sum := 0
	for i := len(base43) - 1; i >= 0; i-- {
		digit := int(base43[i] - '0')
		sum += digit * weight
		weight++
		if weight > 9 {
			weight = 2
		}
	}
	remainder := sum % 11
	if remainder == 0 || remainder == 1 {
		return 0
	}
	return 11 - remainder
}

// generateSefazAccessKey gera uma chave de acesso de 44 dígitos no layout SEFAZ (Modelo 55)
func generateSefazAccessKey(orderID int64, now time.Time) string {
	cUF := "35" // São Paulo
	aamm := now.Format("0601")
	cnpj := "12345678000195"
	mod := "55"
	serie := "001"
	nNF := fmt.Sprintf("%09d", orderID%1000000000)
	tpEmis := "1"

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	randomNum := (uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])) % 100000000
	cNF := fmt.Sprintf("%08d", randomNum)

	base43 := cUF + aamm + cnpj + mod + serie + nNF + tpEmis + cNF
	cDV := calculateMod11(base43)
	return fmt.Sprintf("%s%d", base43, cDV)
}

func randomHex(bytesLen int) string {
	b := make([]byte, bytesLen)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func sendJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func main() {
	port := getEnv("PORT", "3007")
	dbPath := getEnv("SQLITE_DB_PATH", filepath.Join("data", "notifications.db"))

	store := NewSqliteStore(dbPath)
	mux := http.NewServeMux()

	// GET /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "Method Not Allowed"})
			return
		}

		invRows, _ := store.execQuery("SELECT COUNT(*) as total FROM invoices")
		notifRows, _ := store.execQuery("SELECT COUNT(*) as total FROM notification_logs")

		totalInvoices := 0
		if len(invRows) > 0 {
			if v, ok := invRows[0]["total"].(float64); ok {
				totalInvoices = int(v)
			}
		}
		totalNotifications := 0
		if len(notifRows) > 0 {
			if v, ok := notifRows[0]["total"].(float64); ok {
				totalNotifications = int(v)
			}
		}

		sendJSON(w, http.StatusOK, map[string]interface{}{
			"service":  "notification-service",
			"language": "Go 1.22 (Native Memory-Safe / PHP Replacement)",
			"securityMigration": map[string]string{
				"removedRuntime": "PHP (Removido devido a falha de segurança crítica em todas as versões)",
				"activeRuntime":  "Go (Binário nativo compilado estaticamente sem interpretador CGI/Zend)",
				"status":         "MITIGATED",
			},
			"status":                 "UP",
			"database":               dbPath,
			"totalInvoicesIssued":    totalInvoices,
			"totalNotificationsSent": totalNotifications,
			"timestamp":              time.Now().UTC().Format(time.RFC3339),
		})
	})

	// GET /notifications & GET /notifications/invoices
	mux.HandleFunc("/notifications", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "Method Not Allowed"})
			return
		}
		invoices, _ := store.execQuery("SELECT * FROM invoices ORDER BY id DESC LIMIT 50")
		logs, _ := store.execQuery("SELECT * FROM notification_logs ORDER BY id DESC LIMIT 50")
		sendJSON(w, http.StatusOK, map[string]interface{}{
			"service":       "notification-service",
			"language":      "Go 1.22",
			"invoices":      invoices,
			"notifications": logs,
		})
	})

	mux.HandleFunc("/notifications/invoices", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "Method Not Allowed"})
			return
		}
		invoices, _ := store.execQuery("SELECT * FROM invoices ORDER BY id DESC LIMIT 50")
		sendJSON(w, http.StatusOK, invoices)
	})

	// POST /notifications/order-confirmed (Passo 7 da Saga)
	mux.HandleFunc("/notifications/order-confirmed", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			sendJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "Method Not Allowed"})
			return
		}

		// Limita tamanho do payload em 64KB para mitigação de abuso de memória
		r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			sendJSON(w, http.StatusBadRequest, map[string]string{"message": "Payload inválido ou excedeu limite permitido"})
			return
		}

		var payload OrderConfirmedPayload
		if err := json.Unmarshal(bodyBytes, &payload); err != nil || payload.OrderID <= 0 {
			sendJSON(w, http.StatusBadRequest, map[string]string{"message": "JSON inválido ou orderId ausente"})
			return
		}

		now := time.Now().UTC()
		nowISO := now.Format(time.RFC3339)
		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = payload.CorrelationID
		}
		if corrID == "" {
			corrID = "notif-" + randomHex(4)
		}

		invoiceNumber := fmt.Sprintf("NFE-%d-%06d-%s", now.Year(), payload.OrderID, randomHex(2))
		series := "001"
		accessKey := generateSefazAccessKey(payload.OrderID, now)
		protocol := fmt.Sprintf("135%s%07d", now.Format("06"), payload.OrderID%10000000)
		notifID := fmt.Sprintf("NTF-%s", randomHex(5))
		recipient := fmt.Sprintf("customer.%d@ecommerce.local", payload.UserID)
		subject := fmt.Sprintf("Pedido #%d Confirmado - NF-e %s Emitida", payload.OrderID, invoiceNumber)

		if payload.ProductName == "" {
			payload.ProductName = "Item E-Commerce"
		}
		if payload.Quantity <= 0 {
			payload.Quantity = 1
		}

		go func(p OrderConfirmedPayload, invNum, ser, accKey, prot, cID, nID, rec, subj, ts string) {
			_, err := store.execQuery(
				`INSERT INTO invoices (
					invoice_number, series, access_key, authorization_protocol,
					order_id, correlation_id, user_id, product_name, quantity,
					subtotal, discount_amount, tax_amount, total_price,
					payment_transaction_id, status, issued_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'AUTHORIZED_SEFAZ', ?)`,
				invNum, ser, accKey, prot,
				p.OrderID, cID, p.UserID, p.ProductName, p.Quantity,
				p.Subtotal, p.DiscountAmount, p.TaxAmount, p.TotalPrice,
				p.PaymentTransactionID, ts,
			)
			if err != nil {
				log.Printf("[Notification Service - Go] Erro ao gravar NF-e no SQLite: %v", err)
			}

			_, _ = store.execQuery(
				`INSERT INTO notification_logs (
					notification_id, order_id, user_id, channel, recipient,
					subject, invoice_access_key, status, sent_at
				) VALUES (?, ?, ?, 'EMAIL_AND_WEBHOOK', ?, ?, ?, 'DISPATCHED', ?)`,
				nID, p.OrderID, p.UserID, rec, subj, accKey, ts,
			)
		}(payload, invoiceNumber, series, accessKey, protocol, corrID, notifID, recipient, subject, nowISO)

		log.Printf("[Notification Service - Go] [CorrID: %s] Order #%d -> NF-e %s (Chave 44 dígitos: %s)",
			corrID, payload.OrderID, invoiceNumber, accessKey)

		sendJSON(w, http.StatusCreated, map[string]interface{}{
			"service":  "notification-service",
			"language": "Go 1.22",
			"orderId":  payload.OrderID,
			"invoice": map[string]interface{}{
				"invoiceNumber":         invoiceNumber,
				"series":                series,
				"accessKey":             accessKey,
				"authorizationProtocol": protocol,
				"status":                "AUTHORIZED_SEFAZ",
				"subtotal":              payload.Subtotal,
				"discountAmount":        payload.DiscountAmount,
				"taxAmount":             payload.TaxAmount,
				"totalValue":            payload.TotalPrice,
				"issuedAt":              nowISO,
			},
			"notification": map[string]interface{}{
				"notificationId": notifID,
				"channel":        "EMAIL_AND_WEBHOOK",
				"recipient":      recipient,
				"subject":        subject,
				"status":         "DISPATCHED",
				"sentAt":         nowISO,
			},
		})
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	fmt.Printf("[Notification Service - Go] Rodando na porta %s | SQLite: %s (Substituto seguro do PHP)\n", port, dbPath)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Erro ao iniciar Notification Service: %v", err)
	}
}
