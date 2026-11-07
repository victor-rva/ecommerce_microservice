# Encerra todos os microsserviços e o simulador web rodando nas portas 3000 a 3007 e 3080
$ports = @(3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007, 3080)
foreach ($p in $ports) {
    $conns = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        if ($c.OwningProcess -gt 0) {
            Write-Host "Encerrando processo $($c.OwningProcess) na porta $p..." -ForegroundColor Yellow
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
        }
    }
}
Write-Host "Todos os microsserviços e o simulador web (portas 3000..3007 e 3080) foram encerrados." -ForegroundColor Green
