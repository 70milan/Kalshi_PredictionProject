# PredictIQ Mobile & Local Dashboard Guide

This guide explains how to start your React dashboard and FastAPI backend so that you can view and execute trades from **both your Dev Laptop and your Mobile Phone** securely over Tailscale.

## 1. Starting the Services (On Your Dev Laptop)

Open your terminal in VS Code (or your preferred terminal) and start the backend and frontend. You will need **two separate terminal tabs**.

### Terminal 1: Start the Backend (FastAPI)
Run this from the root `predection_project` folder:
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```
*(The `--host 0.0.0.0` part is the magic that allows your phone to connect over Tailscale.)*

### Terminal 2: Start the Frontend (React/Vite)
Open a second terminal tab, navigate into the frontend folder, and start it:
```bash
cd frontend
npm run dev -- --host 0.0.0.0
```

---

## 2. Viewing the Dashboard

### On Your Dev Laptop
Since you are on the machine running the code, simply open your browser and go to:
👉 **http://localhost:5173**

### On Your Mobile Phone
As long as your phone has the **Tailscale app** installed and is connected to your private network:
1. Open Safari or Chrome on your phone.
2. Go to your Dev Laptop's Tailscale IP address at port 5173:
👉 **http://100.86.91.43:5173**

*Note: The React code automatically detects if you are on localhost or your phone and routes all trade executions to the correct backend IP.*

---

## 3. Why is it showing "0" for everything right now?

If you open the dashboard and see zero Active Signals or zero Edge Detected, **this is completely normal.**

Your React dashboard is reading from your Dev Laptop's `data/gold/intelligence_briefs` folder. Since the Python pipeline is running on the Prod server, you are simply waiting for **Syncthing** to download the latest files from Prod to your Dev Laptop. 

Once Syncthing finishes syncing that folder, the React dashboard will automatically detect the new data (it polls every 15 seconds) and populate the screen with all the active trades!
