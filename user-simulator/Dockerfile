FROM python:3.11-slim

WORKDIR /app
COPY . /app

ENV PORT=3080
ENV GATEWAY_URL=http://api-gateway:3000
ENV AUTO_START=true

EXPOSE 3080

CMD ["python", "simulator.py"]
