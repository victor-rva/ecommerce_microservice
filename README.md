# Polyglot E-Commerce Microservices — Distributed Architecture & Paradigm Sandbox

> **Um laboratório prático de Engenharia de Software para explorar Sistemas Distribuídos, Consistência Eventual (Saga Pattern), Persistência Poliglota Isolada e o contraste real entre 7 linguagens de programação e múltiplos paradigmas.**

---

## 1. Visão Geral e Motivação de Engenharia

Na engenharia de software moderna, a maioria dos projetos de estudo limita-se a reproduzir operações CRUD utilizando uma única linguagem e um único banco de dados compartilhado. Embora funcional para aplicações monolíticas simples, esse modelo não expõe os desafios reais de **arquitetura distribuída**: falhas parciais de rede, transações sem *Two-Phase Commit (2PC)*, concorrência sob alta carga, idempotência financeira e adequação de paradigmas de programação ao domínio do problema.

Este projeto nasceu como um **Sandbox Arquitetural Intencionalmente *Over-Engineered*** de um E-Commerce completo. O objetivo não é apenas processar um pedido, mas responder na prática a perguntas fundamentais de engenharia:

1. **Como manter a integridade dos dados quando um único checkout atravessa 7 serviços independentes sem compartilhar banco de dados?**
2. **Como reverter efeitos colaterais (como um estoque já reservado) quando o motor de antifraude ou o gateway de pagamento reprova a compra no meio do caminho?**
3. **Na prática, onde cada paradigma — Funcional Imutável (Clojure), Systems/Memory-Safe sem GC (Rust), Concorrência CSP & Binários Estáticos (Go), Orientação a Objetos Corporativa (Java 17), Event-Loop Assíncrono (TypeScript) e Analítico/Expressivo (Python) — realmente brilha?**

---

## 2. Desenho da Arquitetura

O ecossistema é composto por **8 microsserviços independentes**, escritos em **6 linguagens distintas** (após a substituição de segurança do PHP por Go no `notification-service`), onde **cada serviço possui seu próprio banco de dados SQLite isolado** (*Database-per-Service Pattern*).

```mermaid
flowchart TD
    Client["Cliente / API Consumer"] -->|HTTP :3000| Gateway["api-gateway (Go)\nReverse Proxy, Rate Limiter & Tracing\nSQLite: gateway.db"]

    subgraph DomainServices ["Camada de Domínio & Orquestração de Saga"]
        Gateway -->|/api/products| Product["product-service (TypeScript)\nCatálogo & SKUs\nPorta :3001 | SQLite: products.db"]
        Gateway -->|/api/orders| Order["order-service (TypeScript)\nSaga Orchestrator & Outbox\nPorta :3002 | SQLite: orders.db"]

        Order -->|1. Valida Catálogo| Product
        Order -->|2. Pipeline Imutável de Preço/Cupom| Pricing["pricing-service (Clojure)\nMotor Funcional de Precificação\nPorta :3003 | SQLite: pricing.db"]
        Order -->|3. Reserva em 2 Fases / Rollback| Inventory["inventory-service (Python)\nControle Transacional de Estoque\nPorta :3004 | SQLite: inventory.db"]
        Order -->|4. Score de Risco & Pattern Matching| Fraud["fraud-service (Rust)\nMotor Nativo de Antifraude\nPorta :3005 | SQLite: fraud.db"]
        Order -->|5. Cobrança Idempotente & Ledger| Payment["payment-service (Java 17)\nProcessamento Financeiro\nPorta :3006 | SQLite: payments.db"]
        Order -->|6. Pós-Venda / Emissão de NF-e| Notification["notification-service (Go)\nFaturamento SEFAZ & Notificações\nPorta :3007 | SQLite: notifications.db"]
    end
```

---

## 3. Padrões de Engenharia e Sistemas Distribuídos Aplicados

### 3.1. Database-per-Service com Isolamento Estrito (8 instâncias SQLite)
Um dos anti-padrões mais comuns na migração para microsserviços é o *"Monolito Distribuído"*, onde vários serviços continuam fazendo `JOIN` nas mesmas tabelas de um banco centralizado.

Neste projeto, **nenhum serviço enxerga o banco de dados do outro**:
- Cada microsserviço gerencia exclusivamente seu arquivo `.db` local (`gateway.db`, `products.db`, `orders.db`, `pricing.db`, `inventory.db`, `fraud.db`, `payments.db`, `notifications.db`).
- A escolha do **SQLite (em modo WAL - *Write-Ahead Logging*)** para cada nó permite ter **8 bancos relacionais reais e transacionais (ACID)** rodando simultaneamente com consumo mínimo de memória, facilitando a inspeção individual de schemas, índices e registros gerados em cada etapa.

### 3.2. Padrão Saga Orquestrado & Transações de Compensação
Em um ambiente distribuído, não é viável abrir uma transação SQL global (`BEGIN ... COMMIT`) que atravesse Node.js, Clojure, Python, Rust e Java simultaneamente. Para resolver isso, o `order-service` implementa o **Saga Execution Coordinator (SEC)**:

```mermaid
sequenceDiagram
    autonumber
    participant GW as API Gateway (Go)
    participant OS as Order Saga (TypeScript)
    participant PS as Product (TypeScript)
    participant PR as Pricing (Clojure)
    participant IS as Inventory (Python)
    participant FS as Fraud Engine (Rust)
    participant PAY as Payment Ledger (Java 17)
    participant NF as Invoicing (Go)

    GW->>OS: POST /orders (X-Correlation-ID)
    OS->>PS: 1. Consulta Produto & Preço Base
    OS->>PR: 2. Calcula Descontos em Cascata, Cupom e ICMS/ISS
    OS->>IS: 3. Reserva Estoque (Status: RESERVED)
    OS->>FS: 4. Avalia Sinais de Fraude (Score 0..100)
    alt Reprovado pelo Antifraude (Rust retorna Verdict::Rejected)
        FS-->>OS: 200 OK (verdict: REJECTED, riskScore: 95)
        OS->>IS: COMPENSAÇÃO: POST /inventory/release (Devolve saldo disponível)
        OS-->>GW: 422 Unprocessable Entity (Saga Compensated)
    else Aprovado pelo Antifraude
        FS-->>OS: 200 OK (verdict: APPROVED)
        OS->>PAY: 5. Processa Cobrança + Double-Entry Ledger
        alt Pagamento Recusado (Java retorna 402 DECLINED)
            PAY-->>OS: 402 Payment Required
            OS->>IS: COMPENSAÇÃO: POST /inventory/release (Devolve saldo disponível)
            OS-->>GW: 402 Payment Required (Saga Compensated)
        else Pagamento Aprovado
            PAY-->>OS: 201 Created (transactionId + Ledger Entries)
            OS->>IS: 6. POST /inventory/confirm (Baixa definitiva do estoque)
            OS->>NF: 7. Emite Nota Fiscal (Chave 44 dígitos) & Dispara Notificação
            OS-->>GW: 201 Created (Trilha completa da Saga)
        end
    end
```

### 3.3. Reserva de Estoque em Duas Fases (*Two-Phase Inventory Reservation*)
Para evitar *overselling* (vender o mesmo item para dois clientes concorrentes enquanto o cartão ainda está sendo processado), o `inventory-service` (Python) separa o estoque em duas colunas atômicas: `available_qty` e `reserved_qty`:
1. **`/inventory/reserve`**: Deduz de `available_qty` e incrementa `reserved_qty` com lock transacional (`BEGIN IMMEDIATE`).
2. **`/inventory/confirm`**: Quando o pagamento é aprovado no Java, abate definitivamente de `reserved_qty`.
3. **`/inventory/release` (Compensação)**: Se o Rust detectar fraude ou o Java recusar o cartão, a quantidade reservada volta integralmente para `available_qty`.

### 3.4. Idempotência Financeira e Livro-Razão (*Double-Entry Bookkeeping*)
No `payment-service` (Java 17), cada cobrança recebe uma chave determinística (`idempotency_key`) vinculada ao pedido e ao `X-Correlation-ID`. Além do registro do pagamento, o serviço aplica o princípio contábil de **Partidas Dobradas** na tabela `ledger_entries`: cada centavo cobrado gera simultaneamente um lançamento de **DÉBITO (`DEBIT`)** na conta de recebíveis do cliente e um lançamento de **CRÉDITO (`CREDIT`)** na conta de liquidação da plataforma.

---

## 4. Por que 7 Linguagens? A Engenharia por trás de cada Escolha

A seleção das linguagens não foi aleatória. Cada serviço foi projetado para demonstrar como as características intrínsecas de uma linguagem, seu sistema de tipos e seu *runtime* resolvem um problema específico de engenharia:

### 1. Go (`api-gateway` — Porta `3000`)
- **Problema de Domínio**: Roteamento reverso na borda, controle de *Rate Limiting* concorrente por IP, injeção de `X-Correlation-ID` e agregação de *Health Check* de todos os serviços.
- **Por que Go?** Go foi desenhado no Google exatamente para infraestrutura de rede e proxies (como Traefik, Docker e Kubernetes). Suas *goroutines* com *channels* e `sync.WaitGroup` permitem consultar a saúde dos 7 microsserviços downstream **em paralelo** em poucos milissegundos, com um binário estático que consome menos de 15 MB de RAM.

### 2. TypeScript / Node.js (`product-service` `:3001` & `order-service` `:3002`)
- **Problema de Domínio**: Exposição ágil de catálogo JSON e **Orquestração de I/O da Saga** (coordenar múltiplas chamadas HTTP/eventos encadeados).
- **Por que TypeScript?** O modelo de *Event Loop* single-threaded não-bloqueante do Node.js é excelente para serviços *I/O-bound* que passam 95% do tempo aguardando respostas de rede de outros microsserviços, enquanto o sistema de tipos estrutural do TypeScript modela contratos JSON complexos com produtividade máxima.

### 3. Clojure (`pricing-service` — Porta `3003`)
- **Problema de Domínio**: Motor de cálculo de preços dinâmicos, descontos progressivos por volume (*Tiered Pricing*), aplicação de cupons com teto máximo e cálculo de alíquotas fiscais por estado (`SP`, `RJ`, `MG`, etc.).
- **Por que Clojure (Funcional / Lisp na JVM)?** Motores de regras financeiras e promocionais costumam sofrer com bugs graves causados por mutação acidental de estado (*side-effects*). Em Clojure, **todas as estruturas de dados são nativamente imutáveis**. O cálculo de preço é modelado como um *pipeline* matemático puro utilizando a *threading macro* (`->`), onde o mapa da cotação atravessa funções puras (`apply-volume-tier-discount` -> `apply-coupon-rule` -> `apply-regional-tax` -> `finalize-immutable-quote`), garantindo previsibilidade total e facilidade de teste.

### 4. Python 3.11 (`inventory-service` — Porta `3004`)
- **Problema de Domínio**: Gestão de saldo físico vs. saldo reservado, transações de compensação e indicadores preditivos de ponto de ressuprimento (*reorder threshold*).
- **Por que Python?** A clareza sintática do Python aliada à manipulação nativa de dados e ao suporte embutido ao SQLite permite expressar regras de negócio de logística e análise de estoque com código limpo, legível e de rápida manutenção.

### 5. Rust (`fraud-service` — Porta `3005`)
- **Problema de Domínio**: Análise de risco e detecção de anomalias de fraude em tempo real antes de autorizar a transação financeira.
- **Por que Rust?** Substituindo abordagens pesadas baseadas em máquina virtual, o **Rust** traz dois pilares valiosos para este estudo:
  1. **Modelagem Algébrica Funcional**: Uso de `enum Verdict { Approved, ManualReview, Rejected }` e `enum RiskSignal`, combinados com **Pattern Matching exaustivo (`match`)** verificado em tempo de compilação e iteradores funcionais (`.iter().map().fold()`).
  2. **Systems Programming & Ownership (Zero Garbage Collector)**: Compila diretamente para código de máquina nativo, garantindo segurança de memória e ausência de *data races* sem pausas de *Garbage Collection*, entregando latência previsível na casa de microssegundos.

### 6. Java 17 (`payment-service` — Porta `3006`)
- **Problema de Domínio**: Autorização de pagamento, controle de idempotência transacional e escrituração do livro-razão contábil (*Double-Entry Ledger*).
- **Por que Java 17?** Java permanece o padrão ouro de bancos, adquirentes e fintechs pela maturidade da JVM, tipagem nominal forte, concorrência robusta e evolução moderna da linguagem (como `record` para objetos de transferência imutáveis).

### 7. Go (`notification-service` — Porta `3007` | *Substituto de Segurança do PHP*)
- **Problema de Domínio**: Geração de documentos fiscais pós-venda (simulação de Nota Fiscal Eletrônica com cálculo real de dígito verificador Módulo 11 na chave de acesso de 44 dígitos no padrão SEFAZ) e registro de disparos de e-mail/webhook.
- **Por que migrar de PHP para Go?** Devido a falhas críticas de segurança afetando todas as versões do runtime PHP, o serviço foi integralmente reescrito em **Go (Golang)** — evitando também qualquer dependência de `node_modules` (Node.js/TypeScript). A compilação estática em Go elimina o interpretador CGI/Zend Engine, aplica limites estritos de leitura de payload (`http.MaxBytesReader`) e mantém zero dependências externas.

---

## 5. Matriz Comparativa de Paradigmas e Trade-offs

| Microsserviço | Linguagem | Paradigma Principal | Runtime / Compilação | Modelo de Concorrência | Destaque Técnico no Código |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`api-gateway`** | **Go** | Imperativo / Concorrente (CSP) | Nativo Compilado (Com GC leve) | Goroutines (`M:N` scheduler) + `WaitGroup` | Health-check distribuído paralelo e Reverse Proxy |
| **`product-service`** | **TypeScript** | Multi-paradigma / Assíncrono | V8 Engine (JIT) | Event-Loop Single-Thread (`async`/`await`) | Catálogo REST desacoplado com SQLite WAL |
| **`order-service`** | **TypeScript** | Orientado a Eventos / Saga | V8 Engine (JIT) | I/O Não-bloqueante | Coordenador de Saga, Rollback e Outbox Table |
| **`pricing-service`** | **Clojure** | **Funcional Puro (Lisp)** | JVM Bytecode | Dados Persistentes Imutáveis / `future` | Threading Macros (`->`, `cond->`) sem mutação |
| **`inventory-service`** | **Python 3.11** | Imperativo / Analítico | CPython Bytecode | Multi-threading + SQLite Atomic Locks | Reserva de estoque em 2 fases (`reserve`/`confirm`/`release`) |
| **`fraud-service`** | **Rust** | **Sistemas + Funcional Tipado** | **Nativo (LLVM / Sem GC)** | Threads Nativas + *Ownership & Borrowing* | `enum` Algébricos, `match` exaustivo e `.fold()` |
| **`payment-service`** | **Java 17** | Orientado a Objetos / Enterprise | JVM (JIT HotSpot) | Thread Pool (`ExecutorService`) + Locks | `record` imutável, Idempotência e *Double-Entry Ledger* |
| **`notification-service`** | **Go** | Concorrente / Compilado Seguro | Binário Estático Nativo | Goroutines por Requisição + `sync.Mutex` | Geração de Chave NF-e SEFAZ (44 dígitos Mod-11) sem PHP/CGI |

---

## 6. Como Explorar e Executar o Projeto

Você pode subir todo o ecossistema via **Docker Compose** ou diretamente nos runtimes locais com os scripts automatizados:

```powershell
# Opção 1: Subir os 8 microsserviços localmente no Windows
.\run-ecosystem.ps1

# Executar a suíte de validação da Saga (Happy Path + Rollback de Antifraude + Rollback de Pagamento)
.\test-saga.ps1

# Encerrar todos os serviços (portas 3000..3007)
.\stop-all.ps1

# Opção 2: Subir todo o ambiente containerizado + RabbitMQ
docker compose up --build
```
