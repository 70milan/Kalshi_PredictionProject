# Backend — run this in one terminal
# .\.venv\Scripts\python.exe -m uvicorn api.main:app --host 100.86.91.43 --port 8000

# Frontend — run this in a second terminal (from project root)
# cd frontend
# npm run dev -- --host 100.86.91.43

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'C:\Data Engineering\codeprep\predection_project'; .\.venv\Scripts\python.exe -m uvicorn api.main:app --host 100.86.91.43 --port 8000"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'C:\Data Engineering\codeprep\predection_project\frontend'; npm run dev -- --host 100.86.91.43"
