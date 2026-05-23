# Find processes listening on our prod ports
$ports = 8000, 8001, 5173

foreach ($port in $ports) {
    $pids = netstat -ano | Select-String "LISTENING" | Select-String ":$port " | ForEach-Object {
        ($_ -split '\s+')[-1]
    } | Sort-Object -Unique

    foreach ($processId in $pids) {
        if ($processId -match '^\d+$') {
            $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
            
            # ONLY target python or node to avoid crashing Docker!
            if ($proc -and ($proc.ProcessName -match "python|node")) {
                Write-Host "Gracefully stopping $($proc.ProcessName) (PID $processId) on port $port..."
                # Try graceful termination first (mimics Ctrl+C / close event)
                taskkill /PID $processId 2>$null
                
                # Give the process a moment to shut down gracefully
                Start-Sleep -Seconds 2
                
                # If it's still running after graceful attempt, force kill it
                if (Get-Process -Id $processId -ErrorAction SilentlyContinue) {
                    Write-Host "Process didn't stop gracefully, forcing exit..."
                    taskkill /F /PID $processId 2>$null
                }
            } elseif ($proc) {
                Write-Host "Ignoring $($proc.ProcessName) (PID $processId) on port $port to prevent crashing Docker."
            }
        }
    }
}
