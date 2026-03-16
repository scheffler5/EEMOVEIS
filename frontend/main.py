import os

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI(title="EEMoveis Frontend", version="0.1.0")
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
templates = Jinja2Templates(directory="frontend/templates")


async def fetch_json(path: str, fallback: dict | list) -> dict | list:
    base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{base_url}{path}")
            response.raise_for_status()
            return response.json()
    except Exception:
        return fallback


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    health = await fetch_json("/health", {"status": "indisponivel"})
    insights_payload = await fetch_json("/insights/summary", {"items": []})
    ranking_payload = await fetch_json(
        "/analysis/ranking?limit=5000&offset=0&min_rank=0&mode=geral",
        {"results": []},
    )
    latest_analysis = await fetch_json("/analysis/latest", {"status": "empty"})

    ranking_items = ranking_payload.get("results", []) if isinstance(ranking_payload, dict) else []
    analysis_summary = latest_analysis.get("summary", {}) if isinstance(latest_analysis, dict) else {}

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "health": health,
            "insights": insights_payload.get("items", []),
            "ranking_items": ranking_items,
            "analysis_summary": analysis_summary,
        },
    )
