import express, { Request, Response, NextFunction } from 'express';
import { db, DB_PATH, initDatabase } from './db';

const app = express();
const PORT = Number(process.env.PORT || 3001);

app.disable('x-powered-by');

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

// Inicializa o banco SQLite isolado do product-service
initDatabase();

// Health check para o API Gateway e monitoramento
app.get('/health', (_req: Request, res: Response) => {
    const countResult = db.execSql('SELECT COUNT(*) as count FROM products');
    return res.status(200).json({
        service: 'product-service',
        language: 'TypeScript (Node.js)',
        nodeVersion: process.version,
        securityPatchStatus: {
            cve: 'CVE-2026-111497',
            brotliGuard: 'ACTIVE'
        },
        status: 'UP',
        database: DB_PATH,
        totalProducts: countResult[0]?.count ?? 0,
        timestamp: new Date().toISOString()
    });
});

// Listar todos os produtos (com filtro opcional por categoria)
app.get('/products', (req: Request, res: Response) => {
    const category = req.query.category as string | undefined;
    if (category) {
        const rows = db.execSql(
            'SELECT * FROM products WHERE UPPER(category) = UPPER(?) AND active = 1 ORDER BY id ASC',
            [category]
        );
        return res.status(200).json(rows);
    }

    const rows = db.execSql('SELECT * FROM products WHERE active = 1 ORDER BY id ASC');
    return res.status(200).json(rows);
});

// Obter um produto pelo ID
app.get('/products/:id', (req: Request, res: Response) => {
    const productId = parseInt(String(req.params.id), 10);
    const correlationId = req.header('X-Correlation-ID') || 'N/A';

    const rows = db.execSql('SELECT * FROM products WHERE id = ? AND active = 1', [productId]);
    const product = rows[0];

    if (product) {
        console.log(`[Product Service] [CorrID: ${correlationId}] Produto encontrado: ${product.name} (R$ ${product.price})`);
        return res.status(200).json(product);
    } else {
        console.log(`[Product Service] [CorrID: ${correlationId}] Produto ID ${productId} não encontrado.`);
        return res.status(404).json({ message: 'Produto não encontrado' });
    }
});

// Cadastrar novo produto no catálogo SQLite
app.post('/products', (req: Request, res: Response) => {
    const { id, sku, name, category, price, weightKg } = req.body;
    if (!sku || !name || !category || price === undefined) {
        return res.status(400).json({ message: 'Campos obrigatórios: sku, name, category, price' });
    }

    const now = new Date().toISOString();
    const newId = id || Math.floor(1000 + Math.random() * 9000);

    try {
        db.execSql(
            `INSERT INTO products (id, sku, name, category, price, weightKg, active, createdAt)
             VALUES (?, ?, ?, ?, ?, ?, 1, ?)`,
            [newId, sku, name, category, Number(price), Number(weightKg || 1.0), now]
        );
        const created = db.execSql('SELECT * FROM products WHERE id = ?', [newId])[0];
        return res.status(201).json(created);
    } catch (err: any) {
        return res.status(409).json({ message: 'Erro ao criar produto (ID ou SKU duplicado)', details: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`[Product Service - TypeScript] Rodando na porta ${PORT} | SQLite: ${DB_PATH}`);
});
