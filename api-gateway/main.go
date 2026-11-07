package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SqliteStore gerencia a persistência isolada do API Gateway em gateway.db
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
conn = sqlite3.connect(db_path, timeout=15.0)
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
	_, err := s.execQuery(`
		CREATE TABLE IF NOT EXISTS request_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			correlation_id TEXT NOT NULL,
			method TEXT NOT NULL,
			path TEXT NOT NULL,
			target_service TEXT NOT NULL,
			client_ip TEXT NOT NULL,
			duration_ms INTEGER NOT NULL,
			created_at TEXT NOT NULL
		)
	`)
	if err != nil {
		log.Printf("[API Gateway] Aviso ao inicializar SQLite: %v", err)
	} else {
		log.Printf("[API Gateway] SQLite inicializado em: %s", s.dbPath)
	}
}

func (s *SqliteStore) LogRequestAsync(corrID, method, path, targetService, clientIP string, durationMs int64) {
	go func() {
		_, _ = s.execQuery(
			`INSERT INTO request_logs (correlation_id, method, path, target_service, client_ip, duration_ms, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			corrID, method, path, targetService, clientIP, durationMs, time.Now().UTC().Format(time.RFC3339),
		)
	}()
}

// RateLimiter implementa controle concorrente por IP em Go
type RateLimiter struct {
	mu       sync.Mutex
	requests map[string][]time.Time
	limit    int
	window   time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		requests: make(map[string][]time.Time),
		limit:    limit,
		window:   window,
	}
}

func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)
	valid := make([]time.Time, 0)

	for _, t := range rl.requests[ip] {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	if len(valid) >= rl.limit {
		rl.requests[ip] = valid
		return false
	}

	rl.requests[ip] = append(valid, now)
	return true
}

func generateCorrelationID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "gw-" + hex.EncodeToString(b)
}

type ServiceRoute struct {
	Name       string
	Language   string
	Prefix     string
	StripRoute string
	TargetURL  string
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	port := getEnv("PORT", "3000")
	dbPath := getEnv("SQLITE_DB_PATH", filepath.Join("data", "gateway.db"))

	store := NewSqliteStore(dbPath)
	limiter := NewRateLimiter(1500, time.Minute)

	routes := []ServiceRoute{
		{
			Name:       "product-service",
			Language:   "TypeScript",
			Prefix:     "/api/products",
			StripRoute: "/api",
			TargetURL:  getEnv("PRODUCT_SERVICE_URL", "http://localhost:3001"),
		},
		{
			Name:       "order-service",
			Language:   "TypeScript",
			Prefix:     "/api/orders",
			StripRoute: "/api",
			TargetURL:  getEnv("ORDER_SERVICE_URL", "http://localhost:3002"),
		},
		{
			Name:       "pricing-service",
			Language:   "Clojure",
			Prefix:     "/api/pricing",
			StripRoute: "/api",
			TargetURL:  getEnv("PRICING_SERVICE_URL", "http://localhost:3003"),
		},
		{
			Name:       "inventory-service",
			Language:   "Python",
			Prefix:     "/api/inventory",
			StripRoute: "/api",
			TargetURL:  getEnv("INVENTORY_SERVICE_URL", "http://localhost:3004"),
		},
		{
			Name:       "fraud-service",
			Language:   "Rust",
			Prefix:     "/api/fraud",
			StripRoute: "/api",
			TargetURL:  getEnv("FRAUD_SERVICE_URL", "http://localhost:3005"),
		},
		{
			Name:       "payment-service",
			Language:   "Java 17",
			Prefix:     "/api/payments",
			StripRoute: "/api",
			TargetURL:  getEnv("PAYMENT_SERVICE_URL", "http://localhost:3006"),
		},
		{
			Name:       "notification-service",
			Language:   "Go",
			Prefix:     "/api/notifications",
			StripRoute: "/api",
			TargetURL:  getEnv("NOTIFICATION_SERVICE_URL", "http://localhost:3007"),
		},
	}

	mux := http.NewServeMux()

	// Endpoint de Health Check Distribuído usando Goroutines em paralelo
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		type DownstreamHealth struct {
			Service   string                 `json:"service"`
			Language  string                 `json:"language"`
			TargetURL string                 `json:"targetUrl"`
			Status    string                 `json:"status"`
			LatencyMs int64                  `json:"latencyMs"`
			Details   map[string]interface{} `json:"details,omitempty"`
		}

		results := make([]DownstreamHealth, len(routes))
		var wg sync.WaitGroup
		client := &http.Client{Timeout: 2500 * time.Millisecond}

		for i, rt := range routes {
			wg.Add(1)
			go func(idx int, route ServiceRoute) {
				defer wg.Done()
				start := time.Now()
				resp, err := client.Get(route.TargetURL + "/health")
				latency := time.Since(start).Milliseconds()

				dh := DownstreamHealth{
					Service:   route.Name,
					Language:  route.Language,
					TargetURL: route.TargetURL,
					LatencyMs: latency,
				}

				if err != nil || resp.StatusCode >= 400 {
					dh.Status = "DOWN"
				} else {
					dh.Status = "UP"
					defer resp.Body.Close()
					var details map[string]interface{}
					if err := json.NewDecoder(resp.Body).Decode(&details); err == nil {
						dh.Details = details
					}
				}
				results[idx] = dh
			}(i, rt)
		}

		wg.Wait()

		logRows, _ := store.execQuery("SELECT COUNT(*) as total FROM request_logs")
		totalLogs := 0
		if len(logRows) > 0 {
			if v, ok := logRows[0]["total"].(float64); ok {
				totalLogs = int(v)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"gateway":             "api-gateway",
			"language":            "Go (Golang)",
			"status":              "UP",
			"database":            dbPath,
			"totalProxiedTraffic": totalLogs,
			"microservices":       results,
			"timestamp":           time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Endpoint para inspecionar logs de auditoria gravados no SQLite do Gateway
	mux.HandleFunc("/api/gateway/logs", func(w http.ResponseWriter, r *http.Request) {
		rows, err := store.execQuery("SELECT * FROM request_logs ORDER BY id DESC LIMIT 50")
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(rows)
	})

	// Registra Reverse Proxy para cada microsserviço
	for _, route := range routes {
		rt := route
		target, _ := url.Parse(rt.TargetURL)
		proxy := httputil.NewSingleHostReverseProxy(target)

		originalDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			originalDirector(req)
			req.URL.Path = strings.TrimPrefix(req.URL.Path, rt.StripRoute)
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
			// Remove 'br' (Brotli) de Accept-Encoding na borda para proteger serviços Node.js downstream (CVE-2026-111497)
			if ae := req.Header.Get("Accept-Encoding"); strings.Contains(strings.ToLower(ae), "br") {
				req.Header.Set("Accept-Encoding", "gzip, deflate")
			}
		}

		handler := func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			clientIP := r.RemoteAddr

			// WAF / Proteção de Borda contra CVE-2026-111497 (Node2Shell / Brotli Buffer Overflow)
			ce := strings.ToLower(r.Header.Get("Content-Encoding"))
			te := strings.ToLower(r.Header.Get("Transfer-Encoding"))
			if strings.Contains(ce, "br") || strings.Contains(te, "br") {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnsupportedMediaType)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":   "Unsupported Media Type",
					"message": "Brotli (br) payloads are blocked at API Gateway edge (CVE-2026-111497 mitigation)",
				})
				return
			}

			if !limiter.Allow(clientIP) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"message": "Rate limit excedido no API Gateway (Go)",
				})
				return
			}

			corrID := r.Header.Get("X-Correlation-ID")
			if corrID == "" {
				corrID = generateCorrelationID()
			}
			r.Header.Set("X-Correlation-ID", corrID)
			w.Header().Set("X-Correlation-ID", corrID)
			w.Header().Set("X-Proxied-By", "Go-API-Gateway")

			log.Printf("[Go Gateway] [%s] %s %s -> %s (%s)", corrID, r.Method, r.URL.Path, rt.Name, rt.Language)
			proxy.ServeHTTP(w, r)

			duration := time.Since(start).Milliseconds()
			store.LogRequestAsync(corrID, r.Method, r.URL.Path, rt.Name, clientIP, duration)
		}

		mux.HandleFunc(rt.Prefix, handler)
		mux.HandleFunc(rt.Prefix+"/", handler)
	}

	fmt.Printf("[API Gateway - Go] Rodando na porta %s | SQLite: %s\n", port, dbPath)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Erro ao iniciar API Gateway: %v", err)
	}
}
