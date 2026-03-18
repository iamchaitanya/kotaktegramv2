## Running the app locally

### Backend (serves API + frontend)

From the project root:

```bash
cd /Users/chaitanya/Documents/kotaktegramv2
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

Then open `http://localhost:8000/` in your browser.  
This single process serves both the FastAPI backend and the frontend dashboard.

### Optional: serve frontend only (static files)

If you ever want to serve just the static frontend without the API, from the `frontend` directory:

```bash
cd /Users/chaitanya/Documents/kotaktegramv2/frontend
python -m http.server 3000
```

Then open `http://localhost:3000/` in your browser (note: API calls will still point to `http://localhost:8000` by default).

