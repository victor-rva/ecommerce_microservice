# Script para popular os bancos SQLite, subir o ecossistema e iniciar o Simulador de Usuários + Painel Gerencial
$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "=========================================================================" -ForegroundColor Cyan
Write-Host "  1. Populando Bancos de Dados SQLite (Catálogo, Estoque, Cupons, Bots)  " -ForegroundColor Cyan
Write-Host "=========================================================================" -ForegroundColor Cyan
python (Join-Path $Root "user-simulator\seeder.py")

# Verifica se o API Gateway já está respondendo na porta 3000; se não, sobe os 8 microsserviços
$gwUp = $false
try {
    $null = Invoke-RestMethod -Uri "http://localhost:3000/api/health" -TimeoutSec 2 -ErrorAction Stop
    $gwUp = $true
} catch {
    $gwUp = $false
}

if (-not $gwUp) {
    Write-Host "`nIniciando os 8 microsserviços..." -ForegroundColor Yellow
    & (Join-Path $Root "run-ecosystem.ps1")
} else {
    Write-Host "`n[OK] Os 8 microsserviços já estão online na porta 3000..3007!" -ForegroundColor Green
}

# Encerra instância anterior do simulador na porta 3080 se houver
$simConn = Get-NetTCPConnection -LocalPort 3080 -State Listen -ErrorAction SilentlyContinue
foreach ($c in $simConn) {
    if ($c.OwningProcess -gt 0) {
        Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n=========================================================================" -ForegroundColor Green
Write-Host "  2. Iniciando User Simulator & Painel Gerencial em http://localhost:3080 " -ForegroundColor Green
Write-Host "=========================================================================" -ForegroundColor Green

Start-Process "http://localhost:3080"
python (Join-Path $Root "user-simulator\simulator.py")
