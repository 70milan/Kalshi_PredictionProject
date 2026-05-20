# Kill anything listening on dev ports (backend 8000, frontend 5173)
foreach ($port in 8000, 5173) {
    $pids = netstat -ano | Select-String "LISTENING" | Select-String ":$port " | ForEach-Object {
        ($_ -split '\s+')[-1]
    } | Sort-Object -Unique

    foreach ($processId in $pids) {
        if ($processId -match '^\d+$') {
            Write-Host "Killing PID $processId on port $port"
            taskkill /F /PID $processId 2>$null
        }
    }
}
