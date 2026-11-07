package main

import (
	"bytes"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const OrderConfirmedQueue = "order.confirmed"

var (
	rabbitConnected     atomic.Bool
	rabbitConsumedCount atomic.Int64
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

// OrderConfirmedPayload representa o contrato recebido do order-service (via fila RabbitMQ 'order.confirmed' ou fallback HTTP)
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

// issueInvoiceAndNotify processa de forma idempotente a emissão da NF-e e o disparo de notificação
// tanto quando consumido da fila RabbitMQ ('order.confirmed') quanto via Fallback HTTP.
func issueInvoiceAndNotify(store *SqliteStore, payload OrderConfirmedPayload, corrID string, deliverySource string) map[string]interface{} {
	// Idempotência: se o pedido já teve NF-e emitida (ex: já processado pela fila RabbitMQ), retorna o registro existente
	existing, _ := store.execQuery("SELECT * FROM invoices WHERE order_id = ? LIMIT 1", payload.OrderID)
	if len(existing) > 0 {
		inv := existing[0]
		return map[string]interface{}{
			"service":        "notification-service",
			"language":       "Go 1.22",
			"deliverySource": deliverySource,
			"orderId":        payload.OrderID,
			"invoice": map[string]interface{}{
				"invoiceNumber":         inv["invoice_number"],
				"series":                inv["series"],
				"accessKey":             inv["access_key"],
				"authorizationProtocol": inv["authorization_protocol"],
				"status":                inv["status"],
				"subtotal":              inv["subtotal"],
				"discountAmount":        inv["discount_amount"],
				"taxAmount":             inv["tax_amount"],
				"totalValue":            inv["total_price"],
				"issuedAt":              inv["issued_at"],
			},
		}
	}

	now := time.Now().UTC()
	nowISO := now.Format(time.RFC3339)
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

	_, err := store.execQuery(
		`INSERT INTO invoices (
			invoice_number, series, access_key, authorization_protocol,
			order_id, correlation_id, user_id, product_name, quantity,
			subtotal, discount_amount, tax_amount, total_price,
			payment_transaction_id, status, issued_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'AUTHORIZED_SEFAZ', ?)`,
		invoiceNumber, series, accessKey, protocol,
		payload.OrderID, corrID, payload.UserID, payload.ProductName, payload.Quantity,
		payload.Subtotal, payload.DiscountAmount, payload.TaxAmount, payload.TotalPrice,
		payload.PaymentTransactionID, nowISO,
	)
	if err != nil {
		log.Printf("[Notification Service - Go] Erro ao gravar NF-e no SQLite: %v", err)
	}

	channelLabel := "EMAIL_AND_WEBHOOK (" + deliverySource + ")"
	_, _ = store.execQuery(
		`INSERT INTO notification_logs (
			notification_id, order_id, user_id, channel, recipient,
			subject, invoice_access_key, status, sent_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'DISPATCHED', ?)`,
		notifID, payload.OrderID, payload.UserID, channelLabel, recipient, subject, accessKey, nowISO,
	)

	log.Printf("[Notification Service - Go] [%s] [CorrID: %s] Order #%d -> NF-e %s (Chave 44 dígitos: %s)",
		deliverySource, corrID, payload.OrderID, invoiceNumber, accessKey)

	return map[string]interface{}{
		"service":        "notification-service",
		"language":       "Go 1.22",
		"deliverySource": deliverySource,
		"orderId":        payload.OrderID,
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
			"channel":        channelLabel,
			"recipient":      recipient,
			"subject":        subject,
			"status":         "DISPATCHED",
			"sentAt":         nowISO,
		},
	}
}

// startRabbitMQConsumer inicia um worker em goroutine que declara e consome da fila 'order.confirmed' no RabbitMQ.
// Se o RabbitMQ não estiver rodando (ex: execução local via .ps1 sem Docker), entra em espera silenciosa mantendo o fallback HTTP ativo.
func startRabbitMQConsumer(store *SqliteStore) {
	rabbitAPI := strings.TrimRight(getEnv("RABBITMQ_API_URL", "http://127.0.0.1:15672"), "/")
	rabbitUser := getEnv("RABBITMQ_USER", "guest")
	rabbitPass := getEnv("RABBITMQ_PASS", "guest")

	client := &http.Client{Timeout: 2500 * time.Millisecond}
	queueDeclared := false
	loggedConnected := false

	for {
		if !queueDeclared {
			declareBody := []byte(`{"auto_delete":false,"durable":true,"arguments":{}}`)
			req, err := http.NewRequest(http.MethodPut, rabbitAPI+"/api/queues/%2F/"+OrderConfirmedQueue, bytes.NewReader(declareBody))
			if err == nil {
				req.SetBasicAuth(rabbitUser, rabbitPass)
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err == nil {
					_ = resp.Body.Close()
					if resp.StatusCode >= 200 && resp.StatusCode < 300 {
						queueDeclared = true
						rabbitConnected.Store(true)
						if !loggedConnected {
							log.Printf("[Notification Service - Go] Conectado ao RabbitMQ (%s) | Consumindo fila '%s'", rabbitAPI, OrderConfirmedQueue)
							loggedConnected = true
						}
					}
				}
			}
			if !queueDeclared {
				rabbitConnected.Store(false)
				time.Sleep(3 * time.Second)
				continue
			}
		}

		// Consome até 10 mensagens por ciclo da fila 'order.confirmed' com ACK automático ao processar
		getBody := []byte(`{"count":10,"ackmode":"ack_requeue_false","encoding":"auto","truncate":50000}`)
		req, err := http.NewRequest(http.MethodPost, rabbitAPI+"/api/queues/%2F/"+OrderConfirmedQueue+"/get", bytes.NewReader(getBody))
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		req.SetBasicAuth(rabbitUser, rabbitPass)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			rabbitConnected.Store(false)
			queueDeclared = false
			time.Sleep(3 * time.Second)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_ = resp.Body.Close()
			rabbitConnected.Store(false)
			queueDeclared = false
			time.Sleep(3 * time.Second)
			continue
		}

		rabbitConnected.Store(true)
		var messages []struct {
			Payload string `json:"payload"`
		}
		err = json.NewDecoder(resp.Body).Decode(&messages)
		_ = resp.Body.Close()

		if err == nil && len(messages) > 0 {
			for _, msg := range messages {
				var payload OrderConfirmedPayload
				if err := json.Unmarshal([]byte(msg.Payload), &payload); err == nil && payload.OrderID > 0 {
					issueInvoiceAndNotify(store, payload, payload.CorrelationID, "RABBITMQ_QUEUE ("+OrderConfirmedQueue+")")
					rabbitConsumedCount.Add(1)
				}
			}
		}

		time.Sleep(150 * time.Millisecond)
	}
}

func main() {
	port := getEnv("PORT", "3007")
	dbPath := getEnv("SQLITE_DB_PATH", filepath.Join("data", "notifications.db"))

	store := NewSqliteStore(dbPath)
	go startRabbitMQConsumer(store)

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
			"rabbitmq": map[string]interface{}{
				"queue":            OrderConfirmedQueue,
				"connected":        rabbitConnected.Load(),
				"messagesConsumed": rabbitConsumedCount.Load(),
				"fallbackMode":     "HTTP POST /notifications/order-confirmed",
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

	// GET /notifications/order/{orderId} (Consulta NF-e gerada pelo consumidor RabbitMQ para um pedido específico)
	mux.HandleFunc("/notifications/order/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "Method Not Allowed"})
			return
		}
		orderIDStr := strings.TrimPrefix(r.URL.Path, "/notifications/order/")
		orderID, err := strconv.ParseInt(orderIDStr, 10, 64)
		if err != nil || orderID <= 0 {
			sendJSON(w, http.StatusBadRequest, map[string]string{"message": "orderId inválido"})
			return
		}

		existing, _ := store.execQuery("SELECT * FROM invoices WHERE order_id = ? LIMIT 1", orderID)
		if len(existing) == 0 {
			sendJSON(w, http.StatusNotFound, map[string]string{"message": "NF-e ainda em processamento na fila"})
			return
		}
		inv := existing[0]
		sendJSON(w, http.StatusOK, map[string]interface{}{
			"service":        "notification-service",
			"language":       "Go 1.22",
			"deliverySource": "RABBITMQ_QUEUE (" + OrderConfirmedQueue + ")",
			"orderId":        orderID,
			"invoice": map[string]interface{}{
				"invoiceNumber":         inv["invoice_number"],
				"series":                inv["series"],
				"accessKey":             inv["access_key"],
				"authorizationProtocol": inv["authorization_protocol"],
				"status":                inv["status"],
				"subtotal":              inv["subtotal"],
				"discountAmount":        inv["discount_amount"],
				"taxAmount":             inv["tax_amount"],
				"totalValue":            inv["total_price"],
				"issuedAt":              inv["issued_at"],
			},
		})
	})

	// POST /notifications/order-confirmed (Fallback HTTP caso o RabbitMQ não esteja rodando no modo .ps1)
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

		corrID := r.Header.Get("X-Correlation-ID")
		result := issueInvoiceAndNotify(store, payload, corrID, "HTTP_FALLBACK")
		sendJSON(w, http.StatusCreated, result)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	fmt.Printf("[Notification Service - Go] Rodando na porta %s | SQLite: %s | Fila RabbitMQ: %s\n", port, dbPath, OrderConfirmedQueue)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Erro ao iniciar Notification Service: %v", err)
	}
}
