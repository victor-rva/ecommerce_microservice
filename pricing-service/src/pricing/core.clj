(ns pricing.core
  (:import [com.sun.net.httpserver HttpServer HttpExchange HttpHandler]
           [java.net InetSocketAddress]
           [java.io InputStream OutputStream File]
           [java.nio.charset StandardCharsets]
           [java.time Instant])
  (:require [clojure.string :as str]))

;; =============================================================================
;; CAMADA DE PERSISTÊNCIA ISOLADA (SQLite: pricing.db)
;; =============================================================================

(def db-path
  (or (System/getenv "SQLITE_DB_PATH")
      "data/pricing.db"))

(defn ensure-data-dir! []
  (let [f (File. db-path)
        parent (.getParentFile f)]
    (when (and parent (not (.exists parent)))
      (.mkdirs parent))))

(defn sql-escape [v]
  (cond
    (nil? v) "NULL"
    (number? v) (str v)
    :else (str "'" (str/replace (str v) "'" "''") "'")))

(defn exec-sqlite! [sql]
  (ensure-data-dir!)
  (let [py-code (str "import sqlite3, json, sys\n"
                     "conn = sqlite3.connect(sys.argv[1], timeout=15.0)\n"
                     "conn.execute('PRAGMA journal_mode=WAL;')\n"
                     "cur = conn.cursor()\n"
                     "cur.execute(sys.argv[2])\n"
                     "conn.commit()\n"
                     "conn.close()\n")
        pb (ProcessBuilder. (into-array String ["python" "-c" py-code db-path sql]))
        proc (.start pb)]
    (.waitFor proc)))

(defn init-db! []
  (exec-sqlite!
   "CREATE TABLE IF NOT EXISTS coupons (
      code TEXT PRIMARY KEY,
      discount_pct REAL NOT NULL,
      max_discount REAL NOT NULL,
      description TEXT NOT NULL
    )")
  (exec-sqlite!
   "CREATE TABLE IF NOT EXISTS tax_rules (
      state_code TEXT PRIMARY KEY,
      rate_pct REAL NOT NULL
    )")
  (exec-sqlite!
   "CREATE TABLE IF NOT EXISTS price_quotes (
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
    )")
  (exec-sqlite!
   "INSERT OR IGNORE INTO coupons (code, discount_pct, max_discount, description) VALUES
    ('WELCOME10', 10.0, 500.0, '10% de boas-vindas'),
    ('OVERENGINEER25', 25.0, 2500.0, '25% Sandbox Poliglota Especial'),
    ('VIP50', 50.0, 5000.0, '50% Cliente VIP');")
  (exec-sqlite!
   "INSERT OR IGNORE INTO tax_rules (state_code, rate_pct) VALUES
    ('SP', 12.0), ('RJ', 14.0), ('MG', 10.0), ('PR', 11.0), ('SC', 9.0);")
  (println (str "[Pricing Service - Clojure] SQLite inicializado em: " db-path)))

;; =============================================================================
;; NÚCLEO FUNCIONAL PURO (Zero Efeitos Colaterais / Imutabilidade Total)
;; =============================================================================

(def coupon-catalog
  {"WELCOME10"      {:code "WELCOME10"      :pct 0.10 :max-discount 500.0}
   "OVERENGINEER25" {:code "OVERENGINEER25" :pct 0.25 :max-discount 2500.0}
   "VIP50"          {:code "VIP50"          :pct 0.50 :max-discount 5000.0}})

(def regional-taxes
  {"SP" 0.12
   "RJ" 0.14
   "MG" 0.10
   "PR" 0.11
   "SC" 0.09})

(defn round2 [n]
  (/ (Math/round (* (double n) 100.0)) 100.0))

(defn initial-pricing-context
  "Cria o mapa imutável inicial para o pipeline de precificação."
  [{:keys [orderId productId unitPrice quantity couponCode state]}]
  (let [qty (max 1 (long (or quantity 1)))
        price (double (or unitPrice 0.0))
        subtotal (round2 (* price qty))]
    {:order-id (or orderId 0)
     :product-id (or productId 0)
     :unit-price price
     :quantity qty
     :coupon-code (when (and couponCode (not (str/blank? (str couponCode))))
                    (str/upper-case (str/trim (str couponCode))))
     :state (str/upper-case (or state "SP"))
     :subtotal subtotal
     :volume-discount 0.0
     :coupon-discount 0.0
     :tax-amount 0.0
     :applied-rules []}))

(defn apply-volume-tier-discount
  "Função pura: aplica desconto progressivo por quantidade (Tiered Pricing)."
  [ctx]
  (let [qty (:quantity ctx)
        subtotal (:subtotal ctx)
        tier-rate (cond
                    (>= qty 10) 0.10
                    (>= qty 5)  0.07
                    (>= qty 2)  0.03
                    :else       0.0)
        vol-disc (round2 (* subtotal tier-rate))]
    (cond-> (assoc ctx :volume-discount vol-disc)
      (pos? vol-disc)
      (update :applied-rules conj (str "VOLUME_TIER_" (int (* tier-rate 100)) "PCT")))))

(defn apply-coupon-rule
  "Função pura: aplica cupom promocional respeitando teto máximo de desconto."
  [ctx]
  (if-let [coupon (get coupon-catalog (:coupon-code ctx))]
    (let [base-after-vol (- (:subtotal ctx) (:volume-discount ctx))
          raw-disc (* base-after-vol (:pct coupon))
          capped-disc (round2 (min raw-disc (:max-discount coupon)))]
      (-> ctx
          (assoc :coupon-discount capped-disc)
          (assoc :coupon-applied (:code coupon))
          (update :applied-rules conj (str "COUPON_" (:code coupon)))))
    (assoc ctx :coupon-applied nil)))

(defn apply-regional-tax
  "Função pura: calcula imposto estadual sobre a base líquida."
  [ctx]
  (let [tax-rate (get regional-taxes (:state ctx) 0.12)
        discounted-base (max 0.0 (- (:subtotal ctx)
                                    (:volume-discount ctx)
                                    (:coupon-discount ctx)))
        tax (round2 (* discounted-base tax-rate))]
    (-> ctx
        (assoc :tax-rate-pct (* tax-rate 100.0))
        (assoc :tax-amount tax)
        (update :applied-rules conj (str "TAX_" (:state ctx) "_" (int (* tax-rate 100)) "PCT")))))

(defn finalize-immutable-quote
  "Função pura finalizadora do pipeline."
  [ctx]
  (let [total-discount (round2 (+ (:volume-discount ctx) (:coupon-discount ctx)))
        final-price (round2 (+ (- (:subtotal ctx) total-discount) (:tax-amount ctx)))]
    (assoc ctx
           :total-discount total-discount
           :final-price final-price)))

(defn calculate-price-pipeline
  "Pipeline funcional em Clojure encadeado com threading macro (->)."
  [request-map]
  (-> request-map
      (initial-pricing-context)
      (apply-volume-tier-discount)
      (apply-coupon-rule)
      (apply-regional-tax)
      (finalize-immutable-quote)))

;; =============================================================================
;; PARSER / SERIALIZADOR JSON LEVE & SERVIDOR HTTP
;; =============================================================================

(defn extract-json-field [body field-name default-val]
  (let [pattern (re-pattern (str "\"" field-name "\"\\s*:\\s*(\"([^\"]*)\"|([0-9.]+)|null)"))
        matcher (re-find pattern body)]
    (cond
      (nil? matcher) default-val
      (some? (nth matcher 2)) (nth matcher 2)
      (some? (nth matcher 3)) (Double/parseDouble (nth matcher 3))
      :else default-val)))

(defn parse-pricing-request [body]
  {:orderId    (long (extract-json-field body "orderId" 0))
   :productId  (long (extract-json-field body "productId" 0))
   :unitPrice  (double (extract-json-field body "unitPrice" 0.0))
   :quantity   (long (extract-json-field body "quantity" 1))
   :couponCode (extract-json-field body "couponCode" nil)
   :state      (or (extract-json-field body "state" "SP") "SP")})

(defn quote->json [q]
  (let [rules-json (str "[" (str/join "," (map #(str "\"" % "\"") (:applied-rules q))) "]")
        coupon-str (if (:coupon-applied q) (str "\"" (:coupon-applied q) "\"") "null")]
    (str "{"
         "\"service\":\"pricing-service\","
         "\"language\":\"Clojure (Functional JVM)\","
         "\"orderId\":" (:order-id q) ","
         "\"productId\":" (:product-id q) ","
         "\"subtotal\":" (:subtotal q) ","
         "\"volumeDiscount\":" (:volume-discount q) ","
         "\"couponDiscount\":" (:coupon-discount q) ","
         "\"totalDiscount\":" (:total-discount q) ","
         "\"taxRatePct\":" (:tax-rate-pct q) ","
         "\"taxAmount\":" (:tax-amount q) ","
         "\"finalPrice\":" (:final-price q) ","
         "\"couponApplied\":" coupon-str ","
         "\"state\":\"" (:state q) "\","
         "\"appliedRules\":" rules-json
         "}")))

(defn persist-quote! [q]
  (let [sql (str "INSERT INTO price_quotes (order_id, product_id, quantity, subtotal, volume_discount, coupon_discount, tax_amount, final_price, coupon_code, state_code, created_at) VALUES ("
                 (:order-id q) ", "
                 (:product-id q) ", "
                 (:quantity q) ", "
                 (:subtotal q) ", "
                 (:volume-discount q) ", "
                 (:coupon-discount q) ", "
                 (:tax-amount q) ", "
                 (:final-price q) ", "
                 (sql-escape (:coupon-applied q)) ", "
                 (sql-escape (:state q)) ", "
                 (sql-escape (str (Instant/now))) ")")]
    (future (exec-sqlite! sql))))

(defn send-json! [^HttpExchange exchange status-code json-str]
  (let [bytes (.getBytes (str json-str) StandardCharsets/UTF_8)
        headers (.getResponseHeaders exchange)]
    (.set headers "Content-Type" "application/json; charset=utf-8")
    (.sendResponseHeaders exchange status-code (alength bytes))
    (with-open [^OutputStream os (.getResponseBody exchange)]
      (.write os bytes))))

(defn -main [& _args]
  (init-db!)
  (let [port (Integer/parseInt (or (System/getenv "PORT") "3003"))
        server (HttpServer/create (InetSocketAddress. port) 0)]

    (.createContext server "/health"
      (reify HttpHandler
        (handle [_ exchange]
          (send-json! exchange 200
            (str "{\"service\":\"pricing-service\","
                 "\"language\":\"Clojure 1.11 (Functional Lisp on JVM)\","
                 "\"status\":\"UP\","
                 "\"database\":\"" db-path "\"}")))))

    (.createContext server "/pricing/coupons"
      (reify HttpHandler
        (handle [_ exchange]
          (send-json! exchange 200
            "[{\"code\":\"WELCOME10\",\"discountPct\":10,\"maxDiscount\":500},{\"code\":\"OVERENGINEER25\",\"discountPct\":25,\"maxDiscount\":2500},{\"code\":\"VIP50\",\"discountPct\":50,\"maxDiscount\":5000}]"))))

    (.createContext server "/pricing/calculate"
      (reify HttpHandler
        (handle [_ exchange]
          (if (= "POST" (.getRequestMethod exchange))
            (let [body (slurp (.getRequestBody exchange) :encoding "UTF-8")
                  req-map (parse-pricing-request body)
                  quote (calculate-price-pipeline req-map)]
              (persist-quote! quote)
              (println (str "[Pricing Service - Clojure] Cotação calculada imutavelmente para Order #" (:order-id quote) " -> R$ " (:final-price quote)))
              (send-json! exchange 200 (quote->json quote)))
            (send-json! exchange 405 "{\"message\":\"Method Not Allowed\"}")))))

    (.setExecutor server (java.util.concurrent.Executors/newFixedThreadPool 8))
    (.start server)
    (println (str "[Pricing Service - Clojure] Rodando na porta " port " | SQLite: " db-path))
    @(promise)))
