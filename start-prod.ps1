Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'D:\MilanWork\projects_de\predection_project'; .\.venv\Scripts\python.exe -m uvicorn api.main:app --host 100.67.60.86 --port 8000"

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'D:\MilanWork\projects_de\predection_project\frontend'; npm run dev -- --host 100.67.60.86"

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'D:\MilanWork\projects_de\predection_project'; `$env:READONLY_MODE='true'; .\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8001"