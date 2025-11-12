import express, { Request, Response } from 'express';
import axios from 'axios';

const app = express();
const PORT = 3002;

// URL do outro microsserviço
const PRODUCT_SERVICE_URL = 'http://localhost:3001';

app.use(express.json());

// Endpoint para criar um novo pedido
app.post('/orders', async (req: Request, res: Response) => {
    const { userId, productId, quantity } = req.body;

    if (!userId || !productId || !quantity) {
        return res.status(400).json({ message: 'Dados do pedido incompletos' });
    }

    try {
        console.log(`[Order Service] Recebido pedido para o produto ID: ${productId}. Verificando produto...`);
        
        // COMUNICAÇÃO ENTRE MICROSSERVIÇOS
        // O order-service chama o product-service para obter os detalhes do produto
        const productResponse = await axios.get(`${PRODUCT_SERVICE_URL}/products/${productId}`);
        const product = productResponse.data;

        // Lógica de Negócio do Serviço de Pedidos
        const totalPrice = product.price * quantity;
        
        const newOrder = {
            orderId: Math.floor(Math.random() * 10000),
            userId,
            product,
            quantity,
            totalPrice,
            status: 'CRIADO'
        };

        console.log(`[Order Service] Pedido criado com sucesso:`, newOrder);
        return res.status(201).json(newOrder);

    } catch (error: any) {
        console.error('[Order Service] Erro ao buscar produto:', error.message);
        
        // Se a requisição para o product-service falhar com 404
        if (error.response && error.response.status === 404) {
            return res.status(404).json({ message: 'Produto solicitado não existe.' });
        }

        return res.status(500).json({ message: 'Erro interno no serviço de pedidos.' });
    }
});

app.listen(PORT, () => {
    console.log(`[Order Service] Rodando na porta ${PORT}`);
});