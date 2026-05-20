# Run this on the PROD machine (D:\MilanWork\projects_de\predection_project)
# Requires: predictiq_api_live, predictiq_api_readonly, predictiq_frontend Docker containers STOPPED

$python = ".\.venv\Scripts\python.exe"

Write-Host "Starting PROD services manually..."
Write-Host "Make sure Docker containers for API/frontend are stopped first."
Write-Host ""

# Live API (full access)
Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "& '$python' -m uvicorn api.main:app --host 0.0.0.0 --port 8000"

# Readonly API
Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "`$env:READONLY_MODE='true'; & '$python' -m uvicorn api.main:app --host 0.0.0.0 --port 8001"

# Frontend — production build served via Vite preview (much faster than dev server)
# If dist/ doesn't exist, build first
if (-not (Test-Path ".\frontend\dist")) {
    Write-Host "No frontend/dist found — building first..."
    Push-Location frontend
    npm run build
    Pop-Location
}

Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "cd frontend; npm run preview -- --host 0.0.0.0 --port 5173"

Write-Host ""
Write-Host "API live     : http://100.67.60.86:8000"
Write-Host "API readonly : http://100.67.60.86:8001"
Write-Host "Frontend     : http://100.67.60.86:5173"
Write-Host ""
Write-Host "To rebuild frontend after code changes: cd frontend; npm run build"
Write-Host "To stop everything: .\stop-prod.ps1"
