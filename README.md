# Arquitetura de Microsserviços com TypeScript

É uma demonstração simples de uma arquitetura de microsserviços construída com Node.js e TypeScript. O objetivo é ilustrar como serviços independentes podem se comunicar para realizar uma tarefa de negócio complexa.

O cenário simula um sistema de e-commerce minimalista com dois serviços principais:

1.  **`product-service`**: Gerencia e fornece informações sobre produtos.
2.  **`order-service`**: Processa a criação de novos pedidos, consultando o `product-service` para obter dados dos produtos.

-----

## Arquitetura

O fluxo principal da aplicação é o seguinte:

1.  Um cliente envia uma requisição para o `order-service` para criar um novo pedido, informando o ID do usuário, o ID do produto e a quantidade.
2.  O `order-service` recebe a requisição e, antes de processar o pedido, faz uma chamada HTTP para o `product-service` para validar se o produto existe e obter seu preço.
3.  O `product-service` responde com os detalhes do produto ou com um erro 404 (Não Encontrado).
4.  Com base na resposta, o `order-service` finaliza a criação do pedido, calcula o preço total e retorna os dados do pedido para o cliente.

-----

## Tecnologias Utilizadas

  - **Node.js**: Ambiente de execução JavaScript.
  - **TypeScript**: Superset do JavaScript que adiciona tipagem estática.
  - **Express.js**: Framework web para criar as APIs REST.
  - **Axios**: Cliente HTTP para a comunicação entre os serviços.
  - **ts-node-dev**: Ferramenta para executar o projeto em TypeScript com recarregamento automático durante o desenvolvimento.

-----

## Pré-requisitos

  - [Node.js](https://nodejs.org/en/) (versão 14 ou superior)
  - [npm](https://www.npmjs.com/) ou [Yarn](https://yarnpkg.com/)

-----

## Instalação e Execução

Passos para configurar e rodar o ambiente localmente:

### 1\. Clonar o Repositório

```bash
# Se estiver em um repositório git
git clone <url-do-seu-repositorio>
cd microservices-example
```

### 2\. Configurar o `product-service`

Este serviço é responsável por gerenciar os dados dos produtos.

```bash
# Navegar até a pasta do serviço
cd product-service

# Instalar as dependências
npm install

# Iniciar o serviço
npm start
```

O `product-service` estará rodando em `http://localhost:3001`.

### 3\. Configurar o `order-service`

Abrir um **novo terminal**. Este serviço gerencia os pedidos e se comunica com o `product-service`.

```bash
# Iniciar na raiz do projeto
cd order-service

# Instalar as dependências
npm install

# Iniciar o serviço
npm start
```

O `order-service` estará rodando em `http://localhost:3002`.

Neste ponto, ambos os microsserviços estão online e prontos para receber requisições.

-----

## Testando a Aplicação

Usar uma ferramenta como [Postman](https://www.postman.com/), [Insomnia](https://insomnia.rest/) ou o comando `curl` no terminal para testar os endpoints.

### 1\. Consultar um Produto Existente (no `product-service`)

Para verificar se o serviço de produtos está funcionando, consulte um produto pelo ID.

```bash
curl http://localhost:3001/products/101
```

**Resposta esperada (Status 200 OK):**

```json
{
  "id": 101,
  "name": "Laptop Gamer",
  "price": 6500
}
```

### 2\. Criar um Novo Pedido (no `order-service`)

Este é o teste principal, que demonstra a comunicação entre os serviços.

```bash
curl -X POST http://localhost:3002/orders \
-H "Content-Type: application/json" \
-d '{"userId": 1, "productId": 101, "quantity": 2}'
```

**Fluxo:** O `order-service` (porta 3002) irá chamar o `product-service` (porta 3001) para obter os detalhes do produto `101`.

**Resposta esperada (Status 201 Created):**

```json
{
  "orderId": 45821, // ID do pedido será aleatório
  "userId": 1,
  "product": {
    "id": 101,
    "name": "Laptop Gamer",
    "price": 6500
  },
  "quantity": 2,
  "totalPrice": 13000,
  "status": "CRIADO"
}
```

### 3\. Tentar Criar um Pedido com Produto Inexistente

Este teste verifica o tratamento de erros quando um serviço não encontra a informação no outro.

```bash
curl -X POST http://localhost:3002/orders \
-H "Content-Type: application/json" \
-d '{"userId": 2, "productId": 999, "quantity": 1}'
```

**Resposta esperada (Status 404 Not Found):**

```json
{
  "message": "Produto solicitado não existe."
}
```