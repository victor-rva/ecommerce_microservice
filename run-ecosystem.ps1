# Script para inicializar todos os 8 microsserviços localmente no Windows
$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  Iniciando Ecossistema E-Commerce Poliglota (8 Microsserviços)  " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 0. Popula inicialmente os bancos SQLite com produtos, estoques e usuários simulados
$SeederScript = Join-Path $Root "user-simulator\seeder.py"
if (Test-Path $SeederScript) {
    Write-Host "[Seed] Preenchendo bancos de dados SQLite iniciais..." -ForegroundColor Cyan
    python $SeederScript
}

# 1. Prepara JARs oficiais do Clojure 1.11 para rodar sobre o JDK 17 local
$CljLibDir = Join-Path $Root "pricing-service\.lib"
if (!(Test-Path $CljLibDir)) {
    New-Item -ItemType Directory -Path $CljLibDir -Force | Out-Null
}
$CljJars = @(
    @{ Name = "clojure-1.11.1.jar"; Url = "https://repo1.maven.org/maven2/org/clojure/clojure/1.11.1/clojure-1.11.1.jar" },
    @{ Name = "spec.alpha-0.3.218.jar"; Url = "https://repo1.maven.org/maven2/org/clojure/spec.alpha/0.3.218/spec.alpha-0.3.218.jar" },
    @{ Name = "core.specs.alpha-0.2.62.jar"; Url = "https://repo1.maven.org/maven2/org/clojure/core.specs.alpha/0.2.62/core.specs.alpha-0.2.62.jar" }
)
foreach ($jar in $CljJars) {
    $dest = Join-Path $CljLibDir $jar.Name
    if (!(Test-Path $dest)) {
        Write-Host "[Setup] Baixando runtime oficial Clojure 1.11 ($($jar.Name))..." -ForegroundColor Yellow
        Invoke-WebRequest -Uri $jar.Url -OutFile $dest
    }
}

# 2. Inicia Product Service (TypeScript :3001)
Write-Host "[1/8] Iniciando product-service (TypeScript :3001)..." -ForegroundColor Green
Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "npm start" -WorkingDirectory (Join-Path $Root "product-service") -WindowStyle Hidden

# 3. Inicia Order Service / Saga Orchestrator (TypeScript :3002)
Write-Host "[2/8] Iniciando order-service (TypeScript :3002)..." -ForegroundColor Green
Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "npm start" -WorkingDirectory (Join-Path $Root "order-service") -WindowStyle Hidden

# 4. Inicia Pricing Service (Clojure 1.11 na JVM 17 :3003)
Write-Host "[3/8] Iniciando pricing-service (Clojure 1.11 :3003)..." -ForegroundColor Green
Start-Process -FilePath "java" -ArgumentList "-cp", ".lib/*;src", "clojure.main", "-m", "pricing.core" -WorkingDirectory (Join-Path $Root "pricing-service") -WindowStyle Hidden

# 5. Inicia Inventory Service (Python 3.11 :3004)
Write-Host "[4/8] Iniciando inventory-service (Python 3.11 :3004)..." -ForegroundColor Green
Start-Process -FilePath "python" -ArgumentList "main.py" -WorkingDirectory (Join-Path $Root "inventory-service") -WindowStyle Hidden

# 6. Inicia Fraud Service (Rust :3005)
Write-Host "[5/8] Iniciando fraud-service (Rust :3005)..." -ForegroundColor Green
$RustcCmd = Get-Command "rustc" -ErrorAction SilentlyContinue
if ($RustcCmd) {
    $FraudBin = Join-Path $Root "fraud-service\fraud-server.exe"
    if (!(Test-Path $FraudBin)) {
        & rustc -O (Join-Path $Root "fraud-service\src\main.rs") -o $FraudBin
    }
    Start-Process -FilePath $FraudBin -WorkingDirectory (Join-Path $Root "fraud-service") -WindowStyle Hidden
} else {
    Write-Host "      [Aviso] rustc ainda não instalado no Windows; iniciando runner compatível na porta 3005 (fraud.db)..." -ForegroundColor DarkYellow
    Start-Process -FilePath "python" -ArgumentList "fallback_runner.py" -WorkingDirectory (Join-Path $Root "fraud-service") -WindowStyle Hidden
}

# 7. Inicia Payment Service (Java 17 :3006)
Write-Host "[6/8] Iniciando payment-service (Java 17 :3006)..." -ForegroundColor Green
Start-Process -FilePath "java" -ArgumentList "src/main/java/com/ecommerce/payment/PaymentApplication.java" -WorkingDirectory (Join-Path $Root "payment-service") -WindowStyle Hidden

# 8. Compila e Inicia Notification Service (Go :3007 - Substituto seguro do PHP)
Write-Host "[7/8] Compilando e iniciando notification-service (Go :3007)..." -ForegroundColor Green
$NotifDir = Join-Path $Root "notification-service"
$NotifBin = Join-Path $NotifDir "notification-server.exe"
Push-Location $NotifDir
& go build -o notification-server.exe main.go
Pop-Location
Start-Process -FilePath $NotifBin -WorkingDirectory $NotifDir -WindowStyle Hidden

# 9. Compila e Inicia API Gateway (Go :3000)
Write-Host "[8/8] Compilando e iniciando api-gateway (Go :3000)..." -ForegroundColor Green
$GwDir = Join-Path $Root "api-gateway"
$GwBin = Join-Path $GwDir "gateway.exe"
Push-Location $GwDir
& go build -o gateway.exe main.go
Pop-Location
Start-Process -FilePath $GwBin -WorkingDirectory $GwDir -WindowStyle Hidden

Write-Host "`nAguardando todos os 8 microsserviços ficarem online (portas 3000..3007)..." -ForegroundColor Cyan
for ($i = 1; $i -le 15; $i++) {
    Start-Sleep -Seconds 1
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:3000/api/health" -TimeoutSec 2 -ErrorAction Stop
        $upCount = ($resp.microservices | Where-Object { $_.status -eq "UP" }).Count
        if ($upCount -eq 7) {
            Write-Host "Todos os 8 serviços (Gateway + 7 Microsserviços) estão UP!" -ForegroundColor Green
            break
        }
    } catch {
        # Continua aguardando
    }
}

# 10. Inicia Simulador de Usuários & Painel Gerencial Web (Python :3080)
Write-Host "[9/9] Iniciando user-simulator & Painel Web (Python :3080)..." -ForegroundColor Green
Start-Process -FilePath "python" -ArgumentList "simulator.py" -WorkingDirectory (Join-Path $Root "user-simulator") -WindowStyle Hidden

Write-Host "Pronto! Endpoints disponíveis:" -ForegroundColor Green
Write-Host "  -> Painel Gerencial & Simulador Web: http://localhost:3080" -ForegroundColor Yellow
Write-Host "  -> Health Check Distribuído (API Gateway): http://localhost:3000/api/health" -ForegroundColor Cyan
