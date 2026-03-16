import re
from typing import Any

from fastapi import APIRouter, Query

from backend.db.mongo import get_database
from backend.services.analyzer import get_latest_analysis, get_latest_opportunities, run_market_analysis
from backend.services.analytics import build_price_insights
from shared.models import PropertyModel

router = APIRouter()


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/insights/summary")
async def insights_summary() -> dict[str, list[dict]]:
    database = get_database()
    documents = await database["listings"].find({}, {"_id": 0, "items": 1}).to_list(length=500)

    listings: list[PropertyModel] = []
    seen_keys: set[tuple[str, str]] = set()
    for document in documents:
        for item in document.get("items", []):
            listing = PropertyModel.model_validate(item)
            key = (listing.source_agency, listing.external_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            listings.append(listing)

    insights = build_price_insights(listings)
    return {"items": [insight.model_dump(mode="json") for insight in insights]}


@router.post("/analysis/run")
async def analysis_run(
    discount_threshold: float = Query(default=0.20, ge=0.05, le=0.60),
    min_neighborhood_size: int = Query(default=3, ge=3, le=100),
) -> dict:
    database = get_database()
    result = await run_market_analysis(
        discount_threshold=discount_threshold,
        min_neighborhood_size=min_neighborhood_size,
        database=database,
    )
    return result


@router.get("/analysis/latest")
async def analysis_latest() -> dict:
    database = get_database()
    latest = await get_latest_analysis(database=database)
    if not latest:
        return {"status": "empty", "message": "Nenhuma analise encontrada"}
    return latest


@router.get("/analysis/opportunities")
async def analysis_opportunities(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, list[dict]]:
    database = get_database()
    opportunities = await get_latest_opportunities(limit=limit, database=database)
    return {"items": opportunities}


@router.get("/analysis/ranking")
async def analysis_ranking(
    city: str | None = Query(default=None),
    category: str | None = Query(default=None),
    transaction_type: str | None = Query(default=None, pattern="^(venda|locacao)$"),
    mode: str = Query(default="oportunidades", pattern="^(oportunidades|inflacionados|geral)$"),
    min_rank: float = Query(default=0.0, ge=0.0, le=100.0),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Retorna ranking de investimento da análise mais recente com filtros e paginação."""
    database = get_database()

    source_field = "opportunities" if mode == "oportunidades" else "analyzed_properties"

    match_stage: dict[str, Any] = {"investment_rank_score": {"$gte": min_rank}}
    if city:
        match_stage["city"] = {"$regex": f"^{re.escape(city)}$", "$options": "i"}
    if category:
        match_stage["category"] = {"$regex": f"^{re.escape(category)}$", "$options": "i"}
    if transaction_type:
        match_stage["transaction_type"] = transaction_type
    if mode == "inflacionados":
        match_stage["valuation_status"] = "inflacionado"

    pipeline = [
        {"$sort": {"generated_at": -1, "_id": -1}},
        {"$limit": 1},
        {
            "$project": {
                "analysis_id": {"$toString": "$_id"},
                "analysis_generated_at": "$generated_at",
                source_field: 1,
            }
        },
        {"$unwind": f"${source_field}"},
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        f"${source_field}",
                        {
                            "analysis_id": "$analysis_id",
                            "analysis_generated_at": "$analysis_generated_at",
                        },
                    ]
                }
            }
        },
        {"$match": match_stage},
        {"$sort": {"investment_rank_score": -1, "opportunity_score": -1, "price_per_m2": 1}},
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "results": [{"$skip": offset}, {"$limit": limit}],
            }
        },
    ]

    aggregated = await database["analysis_results"].aggregate(pipeline).to_list(length=1)
    if not aggregated:
        return {
            "count": 0,
            "total": 0,
            "params": {
                "city": city,
                "category": category,
                "transaction_type": transaction_type,
                "mode": mode,
                "min_rank": min_rank,
                "limit": limit,
                "offset": offset,
            },
            "results": [],
        }

    payload = aggregated[0]
    metadata = payload.get("metadata", [])
    results = payload.get("results", [])
    total = int(metadata[0].get("total", 0)) if metadata else 0

    return {
        "count": len(results),
        "total": total,
        "params": {
            "city": city,
            "category": category,
            "transaction_type": transaction_type,
            "mode": mode,
            "min_rank": min_rank,
            "limit": limit,
            "offset": offset,
        },
        "results": results,
    }
