# Atualizado de node:20-alpine (EOL/Vulnerável) para Node 24.22.1 LTS corrigido contra CVE-2026-111497 (Brotli RCE / Node2Shell)
FROM node:24.22.1-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=optional && npm cache clean --force
COPY . .
RUN mkdir -p /app/data && chown -R node:node /app
USER node
EXPOSE 3001
CMD ["npm", "start"]
