# DataDesc FastAPI

## Run locally

```bash
pip install -r requirements.txt
uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

## Endpoints

- `POST /profile` (multipart form) with `files`
- `GET /jobs/{job_id}` status
- `GET /jobs/{job_id}/summary` JSON summary for frontend visuals
- `GET /jobs/{job_id}/download` zip
- `GET /reports/{job_id}/...` static outputs
- `GET /health`

## Frontend integration

Set the frontend endpoint to:

```
http://localhost:8000/profile
```

The response includes `result_url` and `download_url`.
