import express, { Request, Response } from 'express';

const app = express();
const PORT = 3001;

// Banco de dados simulado
const products = [
    { id: 101, name: 'Laptop Gamer', price: 6500 },
    { id: 102, name: 'Mouse Sem Fio', price: 150 },
];

app.use(express.json());

// Endpoint para obter um produto pelo ID
app.get('/products/:id', (req: Request, res: Response) => {
    const productId = parseInt(req.params.id, 10);
    const product = products.find(p => p.id === productId);

    if (product) {
        console.log(`[Product Service] Produto encontrado: ${product.name}`);
        return res.status(200).json(product);
    } else {
        console.log(`[Product Service] Produto com ID ${productId} não encontrado.`);
        return res.status(404).json({ message: 'Produto não encontrado' });
    }
});

app.listen(PORT, () => {
    console.log(`[Product Service] Rodando na porta ${PORT}`);
});