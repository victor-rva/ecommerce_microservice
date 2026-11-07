import express, { Request, Response, NextFunction } from 'express';
import axios from 'axios';
import crypto from 'crypto';
import { db, DB_PATH, initDatabase, logSagaStep } from './db';

const app = express();
const PORT = Number(process.env.PORT || 3002);

app.disable('x-powered-by');

// Hardening do cliente HTTP Axios para nunca negociar compressão Brotli ('br') em respostas (Mitigação CVE-2026-111497)
axios.defaults.headers.common['Accept-Encoding'] = 'gzip, deflate';

// URLs dos microsserviços participantes da Saga
const PRODUCT_SERVICE_URL = process.env.PRODUCT_SERVICE_URL || 'http://127.0.0.1:3001';
const PRICING_SERVICE_URL = process.env.PRICING_SERVICE_URL || 'http://127.0.0.1:3003';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://127.0.0.1:3004';
const FRAUD_SERVICE_URL = process.env.FRAUD_SERVICE_URL || 'http://127.0.0.1:3005';
const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL || 'http://127.0.0.1:3006';
const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL || 'http://127.0.0.1:3007';

// Configuração do RabbitMQ (Fila 'order.confirmed' para o Transactional Outbox do Passo 7)
const RABBITMQ_API_URL = (process.env.RABBITMQ_API_URL || 'http://127.0.0.1:15672').replace(/\/$/, '');
const RABBITMQ_USER = process.env.RABBITMQ_USER || 'guest';
const RABBITMQ_PASS = process.env.RABBITMQ_PASS || 'guest';
const ORDER_CONFIRMED_QUEUE = 'order.confirmed';

let rabbitQueueDeclared = false;
let rabbitPublishedCount = 0;
let rabbitOnline = false;

async function publishToRabbitMQQueue(eventPayload: Record<string, any>): Promise<boolean> {
    const auth = { username: RABBITMQ_USER, password: RABBITMQ_PASS };
    try {
        if (!rabbitQueueDeclared) {
            await axios.put(
                `${RABBITMQ_API_URL}/api/queues/%2F/${ORDER_CONFIRMED_QUEUE}`,
                { auto_delete: false, durable: true, arguments: {} },
                { auth, timeout: 1500 }
            );
            rabbitQueueDeclared = true;
        }

        const pubRes = await axios.post(
            `${RABBITMQ_API_URL}/api/exchanges/%2F/amq.default/publish`,
            {
                properties: {
                    delivery_mode: 2,
                    content_type: 'application/json',
                    correlation_id: eventPayload.correlationId
                },
                routing_key: ORDER_CONFIRMED_QUEUE,
                payload: JSON.stringify(eventPayload),
                payload_encoding: 'string'
            },
            { auth, timeout: 1500 }
        );

        if (pubRes.data && pubRes.data.routed) {
            rabbitOnline = true;
            rabbitPublishedCount++;
            return true;
        }
        return false;
    } catch {
        rabbitQueueDeclared = false;
        rabbitOnline = false;
        return false;
    }
}

async function waitForRabbitConsumerInvoice(orderId: number, headers: Record<string, string>): Promise<any | null> {
    for (let attempt = 1; attempt <= 8; attempt++) {
        await new Promise(resolve => setTimeout(resolve, 120));
        try {
            const res = await axios.get(`${NOTIFICATION_SERVICE_URL}/notifications/order/${orderId}`, {
                headers,
                timeout: 1500
            });
            if (res.status === 200 && res.data?.invoice) {
                return res.data;
            }
        } catch {
            // Continua aguardando o consumidor Go processar a mensagem da fila
        }
    }
    return null;
}

// Worker em background do Transactional Outbox Relay: reprocessa eventos com published = 0
setInterval(async () => {
    try {
        const pendingEvents = db.execSql('SELECT * FROM outbox_events WHERE published = 0');
        for (const ev of pendingEvents) {
            const payload = JSON.parse(ev.payload || '{}');
            const orderId = Number(ev.aggregateId);
            const publishedViaRabbit = await publishToRabbitMQQueue(payload);
            if (publishedViaRabbit) {
                db.execSql('UPDATE outbox_events SET published = ? WHERE aggregateId = ?', [1, orderId]);
                continue;
            }
            try {
                await axios.post(`${NOTIFICATION_SERVICE_URL}/notifications/order-confirmed`, payload, { timeout: 3000 });
                db.execSql('UPDATE outbox_events SET published = ? WHERE aggregateId = ?', [1, orderId]);
            } catch {
                // Permanece published = 0 para próxima tentativa
            }
        }
    } catch {
        // Ignora erros transitórios no loop do Outbox Relay
    }
}, 5000);

// Mitigação em camada de aplicação contra CVE-2026-111497 (Node2Shell / Brotli RCE no zlib)
// Bloqueia payloads com Content-Encoding/Transfer-Encoding 'br' antes do body-parser invocar o decodificador nativo Brotli
app.use((req: Request, res: Response, next: NextFunction) => {
    const contentEncoding = (req.headers['content-encoding'] || '').toString().toLowerCase();
    const transferEncoding = (req.headers['transfer-encoding'] || '').toString().toLowerCase();
    if (contentEncoding.includes('br') || transferEncoding.includes('br')) {
        return res.status(415).json({
            error: 'Unsupported Media Type',
            message: 'Compressão Brotli (br) desabilitada por política de segurança (Mitigação CVE-2026-111497). Utilize gzip ou identity.'
        });
    }
    next();
});

app.use(express.json({ limit: '100kb' }));
initDatabase();

// Health Check
app.get('/health', (_req: Request, res: Response) => {
    const ordersCount = db.execSql('SELECT COUNT(*) as count FROM orders')[0]?.count ?? 0;
    const sagaLogsCount = db.execSql('SELECT COUNT(*) as count FROM saga_logs')[0]?.count ?? 0;
    return res.status(200).json({
        service: 'order-service (Saga Orchestrator)',
        language: 'TypeScript (Node.js)',
        nodeVersion: process.version,
        securityPatchStatus: {
            cve: 'CVE-2026-111497',
            brotliGuard: 'ACTIVE'
        },
        rabbitmqOutbox: {
            queue: ORDER_CONFIRMED_QUEUE,
            rabbitApiUrl: RABBITMQ_API_URL,
            connected: rabbitOnline,
            publishedToQueue: rabbitPublishedCount,
            fallbackMode: 'HTTP POST /notifications/order-confirmed'
        },
        status: 'UP',
        database: DB_PATH,
        totalOrders: ordersCount,
        totalSagaStepsLogged: sagaLogsCount,
        timestamp: new Date().toISOString()
    });
});

// Listar todos os pedidos
app.get('/orders', (_req: Request, res: Response) => {
    const orders = db.execSql('SELECT * FROM orders ORDER BY createdAt DESC');
    return res.status(200).json(orders);
});

// Obter detalhes de um pedido + Trilha completa da Saga e Outbox
app.get('/orders/:id', (req: Request, res: Response) => {
    const orderId = Number(req.params.id);
    const orders = db.execSql('SELECT * FROM orders WHERE orderId = ?', [orderId]);
    if (orders.length === 0) {
        return res.status(404).json({ message: 'Pedido não encontrado' });
    }

    const sagaSteps = db.execSql('SELECT * FROM saga_logs WHERE orderId = ? ORDER BY stepOrder ASC, id ASC', [orderId]).map(s => ({
        ...s,
        details: JSON.parse(s.details || '{}')
    }));
    const outboxEvents = db.execSql('SELECT * FROM outbox_events WHERE aggregateId = ? ORDER BY id ASC', [orderId]);

    return res.status(200).json({
        order: orders[0],
        sagaExecutionTrace: sagaSteps,
        outboxEvents
    });
});

// Endpoint Principal: Orquestrador da Saga de Criação de Pedido
app.post('/orders', async (req: Request, res: Response) => {
    const {
        userId,
        productId,
        quantity,
        couponCode = null,
        state = 'SP',
        paymentMethod = 'CREDIT_CARD'
    } = req.body;

    if (!userId || !productId || !quantity || quantity <= 0) {
        return res.status(400).json({ message: 'Dados do pedido incompletos (userId, productId, quantity > 0 são obrigatórios)' });
    }

    const correlationId = (req.header('X-Correlation-ID') as string) || crypto.randomUUID();
    const orderId = Math.floor(10000 + Math.random() * 90000);
    const now = new Date().toISOString();
    const headers = { 'X-Correlation-ID': correlationId };

    let stepCounter = 1;
    let reservationId: string | null = null;

    try {
        console.log(`\n[Saga Orchestrator] [Order #${orderId}] Iniciando Saga (CorrID: ${correlationId})`);

        // =========================================================================
        // PASSO 1: Consultar Catálogo no product-service (TypeScript :3001)
        // =========================================================================
        let product: any;
        try {
            const productRes = await axios.get(`${PRODUCT_SERVICE_URL}/products/${productId}`, { headers, timeout: 4000 });
            product = productRes.data;
            logSagaStep(orderId, stepCounter++, 'product-service', 'TypeScript', 'FETCH_PRODUCT', 'SUCCESS', {
                productId: product.id,
                name: product.name,
                unitPrice: product.price
            });
        } catch (err: any) {
            logSagaStep(orderId, stepCounter++, 'product-service', 'TypeScript', 'FETCH_PRODUCT', 'FAILED', {
                productId,
                error: err.response?.data || err.message
            });
            if (err.response && err.response.status === 404) {
                return res.status(404).json({ message: 'Produto solicitado não existe.', orderId, correlationId });
            }
            throw new Error(`Falha ao comunicar com product-service: ${err.message}`);
        }

        // Registra o pedido inicialmente como PENDING no SQLite do order-service
        const initialSubtotal = Number((product.price * quantity).toFixed(2));
        db.execSql(
            `INSERT INTO orders (
                orderId, correlationId, userId, productId, productName, quantity,
                unitPrice, subtotal, discountAmount, taxAmount, totalPrice,
                couponCode, stateCode, status, createdAt, updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, 'SAGA_STARTED', ?, ?)`,
            [
                orderId, correlationId, userId, product.id, product.name, quantity,
                product.price, initialSubtotal, initialSubtotal, couponCode, state, now, now
            ]
        );

        // =========================================================================
        // PASSO 2: Calcular Preço Funcional, Cupom e Impostos no pricing-service (Clojure :3003)
        // =========================================================================
        let subtotal = initialSubtotal;
        let discountAmount = 0;
        let taxAmount = 0;
        let finalPrice = initialSubtotal;
        let pricingMetadata: any = {};

        try {
            const pricingRes = await axios.post(
                `${PRICING_SERVICE_URL}/pricing/calculate`,
                {
                    orderId,
                    productId: product.id,
                    category: product.category || 'ELECTRONICS',
                    unitPrice: product.price,
                    quantity,
                    couponCode,
                    state
                },
                { headers, timeout: 5000 }
            );
            const quote = pricingRes.data;
            subtotal = Number(quote.subtotal ?? initialSubtotal);
            discountAmount = Number(quote.totalDiscount ?? 0);
            taxAmount = Number(quote.taxAmount ?? 0);
            finalPrice = Number(quote.finalPrice ?? initialSubtotal);
            pricingMetadata = quote;

            logSagaStep(orderId, stepCounter++, 'pricing-service', 'Clojure', 'CALCULATE_IMMUTABLE_PRICE', 'SUCCESS', quote);
        } catch (err: any) {
            logSagaStep(orderId, stepCounter++, 'pricing-service', 'Clojure', 'CALCULATE_IMMUTABLE_PRICE', 'FAILED', {
                fallbackUsed: true,
                reason: err.message
            });
        }

        db.execSql(
            `UPDATE orders SET subtotal = ?, discountAmount = ?, taxAmount = ?, totalPrice = ?, status = 'PRICE_CALCULATED', updatedAt = ? WHERE orderId = ?`,
            [subtotal, discountAmount, taxAmount, finalPrice, new Date().toISOString(), orderId]
        );

        // =========================================================================
        // PASSO 3: Reservar Estoque em 2 Fases no inventory-service (Python :3004)
        // =========================================================================
        try {
            const invRes = await axios.post(
                `${INVENTORY_SERVICE_URL}/inventory/reserve`,
                { orderId, productId: product.id, quantity },
                { headers, timeout: 4000 }
            );
            reservationId = invRes.data.reservationId;
            logSagaStep(orderId, stepCounter++, 'inventory-service', 'Python', 'RESERVE_STOCK', 'SUCCESS', invRes.data);
        } catch (err: any) {
            const errDetail = err.response?.data || { error: err.message };
            logSagaStep(orderId, stepCounter++, 'inventory-service', 'Python', 'RESERVE_STOCK', 'FAILED', errDetail);
            db.execSql(`UPDATE orders SET status = 'REJECTED_OUT_OF_STOCK', updatedAt = ? WHERE orderId = ?`, [new Date().toISOString(), orderId]);

            return res.status(409).json({
                message: 'Estoque insuficiente para concluir o pedido.',
                orderId,
                correlationId,
                status: 'REJECTED_OUT_OF_STOCK',
                inventoryError: errDetail
            });
        }

        db.execSql(
            `UPDATE orders SET reservationId = ?, status = 'STOCK_RESERVED', updatedAt = ? WHERE orderId = ?`,
            [reservationId, new Date().toISOString(), orderId]
        );

        // =========================================================================
        // PASSO 4: Avaliar Risco e Antifraude no fraud-service (Rust :3005)
        // =========================================================================
        let fraudResult: any;
        try {
            const fraudRes = await axios.post(
                `${FRAUD_SERVICE_URL}/fraud/evaluate`,
                {
                    orderId,
                    userId,
                    productId: product.id,
                    quantity,
                    totalAmount: finalPrice
                },
                { headers, timeout: 4000 }
            );
            fraudResult = fraudRes.data;

            if (fraudResult.verdict === 'REJECTED') {
                logSagaStep(orderId, stepCounter++, 'fraud-service', 'Rust', 'EVALUATE_FRAUD_RISK', 'FAILED', fraudResult);

                // TRANSAÇÃO DE COMPENSAÇÃO DA SAGA: Liberar reserva de estoque no Python!
                await compensateInventory(orderId, reservationId!, stepCounter++, headers, 'FRAUD_REJECTED');

                db.execSql(
                    `UPDATE orders SET fraudVerdict = ?, fraudScore = ?, status = 'CANCELLED_FRAUD_DETECTED', updatedAt = ? WHERE orderId = ?`,
                    [fraudResult.verdict, fraudResult.riskScore ?? 100, new Date().toISOString(), orderId]
                );

                return res.status(422).json({
                    message: 'Pedido reprovado pelo motor de antifraude (Rust). Reserva de estoque revertida via Saga Compensation.',
                    orderId,
                    correlationId,
                    status: 'CANCELLED_FRAUD_DETECTED',
                    fraudAnalysis: fraudResult,
                    sagaCompensated: true
                });
            }

            logSagaStep(orderId, stepCounter++, 'fraud-service', 'Rust', 'EVALUATE_FRAUD_RISK', 'SUCCESS', fraudResult);
            db.execSql(
                `UPDATE orders SET fraudVerdict = ?, fraudScore = ?, status = 'FRAUD_APPROVED', updatedAt = ? WHERE orderId = ?`,
                [fraudResult.verdict, fraudResult.riskScore ?? 0, new Date().toISOString(), orderId]
            );
        } catch (err: any) {
            logSagaStep(orderId, stepCounter++, 'fraud-service', 'Rust', 'EVALUATE_FRAUD_RISK', 'FAILED', { error: err.message });
            await compensateInventory(orderId, reservationId!, stepCounter++, headers, 'FRAUD_SERVICE_ERROR');
            db.execSql(`UPDATE orders SET status = 'FAILED_FRAUD_CHECK', updatedAt = ? WHERE orderId = ?`, [new Date().toISOString(), orderId]);
            return res.status(503).json({ message: 'Serviço Antifraude indisponível. Estoque revertido.', orderId });
        }

        // =========================================================================
        // PASSO 5: Processar Pagamento e Ledger no payment-service (Java 17 :3006)
        // =========================================================================
        let paymentData: any;
        try {
            const payRes = await axios.post(
                `${PAYMENT_SERVICE_URL}/payments/charge`,
                {
                    orderId,
                    userId,
                    amount: finalPrice,
                    paymentMethod,
                    idempotencyKey: `ORD-${orderId}-${correlationId}`
                },
                { headers, timeout: 5000 }
            );
            paymentData = payRes.data;

            if (paymentData.status !== 'APPROVED') {
                logSagaStep(orderId, stepCounter++, 'payment-service', 'Java 17', 'CHARGE_PAYMENT', 'FAILED', paymentData);
                await compensateInventory(orderId, reservationId!, stepCounter++, headers, 'PAYMENT_DECLINED');
                db.execSql(`UPDATE orders SET status = 'CANCELLED_PAYMENT_DECLINED', updatedAt = ? WHERE orderId = ?`, [new Date().toISOString(), orderId]);

                return res.status(402).json({
                    message: 'Pagamento recusado pelo serviço financeiro (Java). Estoque revertido via Saga Compensation.',
                    orderId,
                    correlationId,
                    status: 'CANCELLED_PAYMENT_DECLINED',
                    payment: paymentData,
                    sagaCompensated: true
                });
            }

            logSagaStep(orderId, stepCounter++, 'payment-service', 'Java 17', 'CHARGE_PAYMENT', 'SUCCESS', paymentData);
        } catch (err: any) {
            const errDetail = err.response?.data || { error: err.message };
            logSagaStep(orderId, stepCounter++, 'payment-service', 'Java 17', 'CHARGE_PAYMENT', 'FAILED', errDetail);
            await compensateInventory(orderId, reservationId!, stepCounter++, headers, 'PAYMENT_ERROR');
            db.execSql(`UPDATE orders SET status = 'CANCELLED_PAYMENT_DECLINED', updatedAt = ? WHERE orderId = ?`, [new Date().toISOString(), orderId]);

            return res.status(402).json({
                message: 'Falha no processamento do pagamento. Estoque revertido via Saga Compensation.',
                orderId,
                correlationId,
                status: 'CANCELLED_PAYMENT_DECLINED',
                paymentError: errDetail,
                sagaCompensated: true
            });
        }

        // =========================================================================
        // PASSO 6: Confirmar Baixa Definitiva de Estoque no Python (:3004)
        // =========================================================================
        try {
            const confirmRes = await axios.post(
                `${INVENTORY_SERVICE_URL}/inventory/confirm`,
                { reservationId, orderId },
                { headers, timeout: 4000 }
            );
            logSagaStep(orderId, stepCounter++, 'inventory-service', 'Python', 'CONFIRM_STOCK_COMMIT', 'SUCCESS', confirmRes.data);
        } catch (err: any) {
            logSagaStep(orderId, stepCounter++, 'inventory-service', 'Python', 'CONFIRM_STOCK_COMMIT', 'FAILED', { error: err.message });
        }

        // =========================================================================
        // PASSO 7: Transactional Outbox -> Fila RabbitMQ ('order.confirmed') -> Go (:3007)
        // Mantém Fallback HTTP automático caso o RabbitMQ não esteja rodando no modo .ps1
        // =========================================================================
        let invoiceData: any = null;
        const eventPayload = {
            event: 'OrderConfirmedEvent',
            orderId,
            correlationId,
            userId,
            productName: product.name,
            quantity,
            subtotal,
            discountAmount,
            taxAmount,
            totalPrice: finalPrice,
            paymentTransactionId: paymentData.transactionId,
            confirmedAt: new Date().toISOString()
        };

        // 7.1. Grava evento no Transactional Outbox inicialmente como published = 0
        db.execSql(
            `INSERT INTO outbox_events (aggregateId, eventType, payload, published, createdAt)
             VALUES (?, 'OrderConfirmedEvent', ?, ?, ?)`,
            [orderId, JSON.stringify(eventPayload), 0, new Date().toISOString()]
        );

        // 7.2. Tenta publicar na fila real do RabbitMQ ('order.confirmed')
        const publishedToRabbit = await publishToRabbitMQQueue(eventPayload);

        if (publishedToRabbit) {
            db.execSql('UPDATE outbox_events SET published = ? WHERE aggregateId = ?', [1, orderId]);

            // Aguarda brevemente o consumidor Go processar a mensagem da fila 'order.confirmed'
            invoiceData = await waitForRabbitConsumerInvoice(orderId, headers);

            if (!invoiceData) {
                // Caso o ciclo do consumidor Go ainda não tenha fechado, garante a emissão idempotente via HTTP
                try {
                    const notifRes = await axios.post(
                        `${NOTIFICATION_SERVICE_URL}/notifications/order-confirmed`,
                        eventPayload,
                        { headers, timeout: 4000 }
                    );
                    invoiceData = notifRes.data;
                } catch {
                    // O consumidor do RabbitMQ processará em background
                }
            }

            logSagaStep(orderId, stepCounter++, 'notification-service', 'Go', 'ISSUE_NFE_VIA_RABBITMQ', 'SUCCESS', {
                broker: 'RabbitMQ',
                queue: ORDER_CONFIRMED_QUEUE,
                outboxPublished: true,
                ...(invoiceData || {})
            });
        } else {
            // 7.3. Fallback HTTP automático (quando executado localmente via .ps1 sem o container do RabbitMQ)
            try {
                const notifRes = await axios.post(
                    `${NOTIFICATION_SERVICE_URL}/notifications/order-confirmed`,
                    eventPayload,
                    { headers, timeout: 4000 }
                );
                invoiceData = notifRes.data;
                db.execSql('UPDATE outbox_events SET published = ? WHERE aggregateId = ?', [1, orderId]);
                logSagaStep(orderId, stepCounter++, 'notification-service', 'Go', 'ISSUE_NFE_HTTP_FALLBACK', 'SUCCESS', {
                    deliveryMode: 'HTTP_FALLBACK (RabbitMQ Offline)',
                    outboxPublished: true,
                    ...invoiceData
                });
            } catch (err: any) {
                logSagaStep(orderId, stepCounter++, 'notification-service', 'Go', 'ISSUE_NFE_AND_NOTIFY', 'FAILED', {
                    note: 'Outbox gravado (published=0) para reprocessamento assíncrono pelo Outbox Relay',
                    error: err.message
                });
            }
        }

        // Atualiza pedido para CONFIRMED
        db.execSql(
            `UPDATE orders
             SET paymentTransactionId = ?, invoiceNumber = ?, status = 'CONFIRMED', updatedAt = ?
             WHERE orderId = ?`,
            [paymentData.transactionId, invoiceData?.invoice?.invoiceNumber || null, new Date().toISOString(), orderId]
        );

        const sagaTrace = db.execSql('SELECT * FROM saga_logs WHERE orderId = ? ORDER BY stepOrder ASC', [orderId]).map(s => ({
            step: s.stepOrder,
            service: s.serviceName,
            language: s.language,
            action: s.action,
            status: s.status
        }));

        return res.status(201).json({
            orderId,
            correlationId,
            status: 'CONFIRMED',
            userId,
            product: {
                id: product.id,
                sku: product.sku,
                name: product.name,
                unitPrice: product.price
            },
            quantity,
            pricing: {
                subtotal,
                discountAmount,
                taxAmount,
                totalPrice: finalPrice,
                couponApplied: pricingMetadata.couponApplied || couponCode,
                state
            },
            inventory: {
                reservationId,
                status: 'COMMITTED'
            },
            fraudAnalysis: {
                verdict: fraudResult.verdict,
                riskScore: fraudResult.riskScore
            },
            payment: {
                transactionId: paymentData.transactionId,
                status: paymentData.status,
                method: paymentMethod
            },
            messaging: {
                outboxQueue: ORDER_CONFIRMED_QUEUE,
                deliveryMode: publishedToRabbit ? 'RABBITMQ_QUEUE' : 'HTTP_FALLBACK'
            },
            invoice: invoiceData?.invoice || null,
            sagaSteps: sagaTrace
        });

    } catch (error: any) {
        console.error(`[Order Service] Erro crítico na Saga do pedido #${orderId}:`, error.message);
        return res.status(500).json({
            message: 'Erro interno na orquestração da Saga de pedidos.',
            orderId,
            correlationId,
            details: error.message
        });
    }
});

async function compensateInventory(
    orderId: number,
    reservationId: string,
    stepOrder: number,
    headers: Record<string, string>,
    reason: string
): Promise<void> {
    try {
        const releaseRes = await axios.post(
            `${INVENTORY_SERVICE_URL}/inventory/release`,
            { reservationId, orderId, reason },
            { headers, timeout: 4000 }
        );
        logSagaStep(orderId, stepOrder, 'inventory-service', 'Python', 'COMPENSATE_RELEASE_STOCK', 'COMPENSATED', releaseRes.data);
    } catch (err: any) {
        logSagaStep(orderId, stepOrder, 'inventory-service', 'Python', 'COMPENSATE_RELEASE_STOCK', 'FAILED', {
            reservationId,
            error: err.message
        });
    }
}

app.listen(PORT, () => {
    console.log(`[Order Service / Saga Orchestrator - TypeScript] Rodando na porta ${PORT} | SQLite: ${DB_PATH} | Fila Outbox: ${ORDER_CONFIRMED_QUEUE}`);
});
