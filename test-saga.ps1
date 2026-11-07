# Suite de Testes Ponta a Ponta da Saga Poliglota
$ErrorActionPreference = "Stop"

Write-Host "=======================================================================" -ForegroundColor Cyan
Write-Host "  TESTE E2E: ARQUITETURA DE MICROSSERVIÇOS POLIGLOTA & SAGA PATTERN    " -ForegroundColor Cyan
Write-Host "=======================================================================" -ForegroundColor Cyan

Write-Host "`n[1] Consultando Health Check Distribuído via API Gateway (Go :3000)..." -ForegroundColor Yellow
$health = Invoke-RestMethod -Uri "http://localhost:3000/api/health" -Method Get
$health | ConvertTo-Json -Depth 4 | Write-Host

Write-Host "`n[2] CENÁRIO 1 (Happy Path): Pedido com Cupom OVERENGINEER25 (Passando pelas 7 linguagens)..." -ForegroundColor Green
$happyOrderBody = @{
    userId        = 42
    productId     = 101
    quantity      = 2
    couponCode    = "OVERENGINEER25"
    state         = "SP"
    paymentMethod = "CREDIT_CARD"
} | ConvertTo-Json

$happyResult = Invoke-RestMethod -Uri "http://localhost:3000/api/orders" -Method Post -ContentType "application/json" -Body $happyOrderBody
$happyResult | ConvertTo-Json -Depth 5 | Write-Host

Write-Host "`n[3] CENÁRIO 2 (Saga Compensation): Pedido suspeito (userId=123) bloqueado pelo Rust Antifraude -> Estorno de estoque no Python..." -ForegroundColor Magenta
$fraudOrderBody = @{
    userId        = 123
    productId     = 101
    quantity      = 3
    couponCode    = "WELCOME10"
    state         = "RJ"
    paymentMethod = "CREDIT_CARD"
} | ConvertTo-Json

try {
    Invoke-RestMethod -Uri "http://localhost:3000/api/orders" -Method Post -ContentType "application/json" -Body $fraudOrderBody
} catch {
    $errStream = $_.Exception.Response.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($errStream)
    $errBody = $reader.ReadToEnd()
    Write-Host "Resposta esperada de Rollback da Saga (HTTP 422):" -ForegroundColor Yellow
    Write-Host $errBody
}

Write-Host "`n[4] CENÁRIO 3 (Saga Compensation): Pagamento Recusado no Java (userId=789) -> Estorno de estoque no Python..." -ForegroundColor Magenta
$declinedPayBody = @{
    userId        = 789
    productId     = 102
    quantity      = 5
    state         = "MG"
    paymentMethod = "DECLINED_CARD"
} | ConvertTo-Json

try {
    Invoke-RestMethod -Uri "http://localhost:3000/api/orders" -Method Post -ContentType "application/json" -Body $declinedPayBody
} catch {
    $errStream = $_.Exception.Response.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($errStream)
    $errBody = $reader.ReadToEnd()
    Write-Host "Resposta esperada de Pagamento Recusado + Rollback (HTTP 402):" -ForegroundColor Yellow
    Write-Host $errBody
}

Write-Host "`n[5] Verificando Saldo de Estoque no Python após confirmações e compensações..." -ForegroundColor Cyan
$inv = Invoke-RestMethod -Uri "http://localhost:3000/api/inventory/101" -Method Get
$inv | ConvertTo-Json -Depth 3 | Write-Host
