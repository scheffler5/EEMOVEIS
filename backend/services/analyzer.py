import asyncio
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorDatabase

from backend.db.mongo import get_database


_analysis_lock = asyncio.Lock()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: Any, digits: int = 2) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _serialize_for_json(document: dict[str, Any]) -> dict[str, Any]:
    serialized = {**document}
    if "_id" in serialized:
        serialized["_id"] = str(serialized["_id"])
    return serialized


def _infer_transaction_type(item: dict[str, Any]) -> str:
    raw_value = str(item.get("transaction_type") or "").strip().lower()
    if raw_value in {"venda", "locacao", "aluguel"}:
        return "locacao" if raw_value in {"locacao", "aluguel"} else "venda"

    url = str(item.get("url") or "").lower()
    if "/locacao/" in url or "/aluguel/" in url:
        return "locacao"
    if "/venda/" in url:
        return "venda"
    return "venda"


def _transaction_label(transaction_type: str) -> str:
    return "Alugar" if transaction_type == "locacao" else "Comprar"


async def _get_listings_snapshot(database: AsyncIOMotorDatabase) -> dict[str, Any]:
    pipeline = [
        {
            "$group": {
                "_id": None,
                "batch_count": {"$sum": 1},
                "latest_listing_ingested_at": {"$max": "$ingested_at"},
                "total_items": {"$sum": "$total_items"},
            }
        }
    ]
    rows = await database["listings"].aggregate(pipeline).to_list(length=1)
    if not rows:
        return {
            "batch_count": 0,
            "latest_listing_ingested_at": None,
            "total_items": 0,
        }

    row = rows[0]
    return {
        "batch_count": int(row.get("batch_count") or 0),
        "latest_listing_ingested_at": row.get("latest_listing_ingested_at"),
        "total_items": int(row.get("total_items") or 0),
    }


async def _load_unique_properties(database: AsyncIOMotorDatabase) -> list[dict[str, Any]]:
    documents = await database["listings"].find({}, {"_id": 0, "items": 1}).to_list(length=20000)

    seen_keys: set[tuple[str, str]] = set()
    unique_items: list[dict[str, Any]] = []
    for document in documents:
        for item in document.get("items", []):
            source_agency = str(item.get("source_agency") or "")
            external_id = str(item.get("external_id") or "")
            if not source_agency or not external_id:
                continue

            key = (source_agency, external_id)
            if key in seen_keys:
                continue

            seen_keys.add(key)
            unique_items.append(
                {
                    **item,
                    "transaction_type": _infer_transaction_type(item),
                    "transaction_label": _transaction_label(_infer_transaction_type(item)),
                }
            )

    return unique_items


async def run_market_analysis(
    *,
    discount_threshold: float = 0.20,
    min_neighborhood_size: int = 3,
    database: AsyncIOMotorDatabase | None = None,
) -> dict[str, Any]:
    async with _analysis_lock:
        if database is None:
            database = get_database()
        properties = await _load_unique_properties(database)
        listings_snapshot = await _get_listings_snapshot(database)

        if not properties:
            result = {
                "generated_at": datetime.now(timezone.utc),
                "config": {
                    "discount_threshold": discount_threshold,
                    "min_neighborhood_size": min_neighborhood_size,
                    "outlier_quantiles": [0.01, 0.99],
                    "group_cols": ["city", "neighborhood", "category", "transaction_type"],
                },
                "source_snapshot": listings_snapshot,
                "summary": {
                    "total_input_properties": 0,
                    "valid_after_basic_cleaning": 0,
                    "valid_after_outlier_filter": 0,
                    "segments_analyzed": 0,
                    "opportunities_found": 0,
                    "sale_opportunities_found": 0,
                    "rental_opportunities_found": 0,
                    "yield_covered_sales": 0,
                },
                "segment_stats": [],
                "neighborhood_stats": [],
                "opportunities": [],
            }
            insert_result = await database["analysis_results"].insert_one(result)
            return {"analysis_id": str(insert_result.inserted_id), **result["summary"]}

        frame = pd.DataFrame(properties)
        frame["price"] = pd.to_numeric(frame.get("price"), errors="coerce")
        frame["area_total_m2"] = pd.to_numeric(frame.get("area_total_m2"), errors="coerce")

        frame = frame[
            (frame["price"].notna())
            & (frame["price"] > 0)
            & (frame["area_total_m2"].notna())
            & (frame["area_total_m2"] > 0)
            & (frame.get("neighborhood").notna())
            & (frame.get("city").notna())
            & (frame.get("category").notna())
            & (frame.get("transaction_type").notna())
        ].copy()

        if frame.empty:
            result = {
                "generated_at": datetime.now(timezone.utc),
                "config": {
                    "discount_threshold": discount_threshold,
                    "min_neighborhood_size": min_neighborhood_size,
                    "outlier_quantiles": [0.01, 0.99],
                    "group_cols": ["city", "neighborhood", "category", "transaction_type"],
                },
                "source_snapshot": listings_snapshot,
                "summary": {
                    "total_input_properties": len(properties),
                    "valid_after_basic_cleaning": 0,
                    "valid_after_outlier_filter": 0,
                    "segments_analyzed": 0,
                    "opportunities_found": 0,
                    "sale_opportunities_found": 0,
                    "rental_opportunities_found": 0,
                    "yield_covered_sales": 0,
                },
                "segment_stats": [],
                "neighborhood_stats": [],
                "opportunities": [],
            }
            insert_result = await database["analysis_results"].insert_one(result)
            return {"analysis_id": str(insert_result.inserted_id), **result["summary"]}

        frame["price_per_m2"] = frame["price"] / frame["area_total_m2"]

        filtered_frames: list[pd.DataFrame] = []
        for transaction_type, transaction_frame in frame.groupby("transaction_type", dropna=False):
            if transaction_frame.empty:
                continue
            low_quantile = transaction_frame["price_per_m2"].quantile(0.01)
            high_quantile = transaction_frame["price_per_m2"].quantile(0.99)
            filtered_frames.append(
                transaction_frame[
                    (transaction_frame["price_per_m2"] >= low_quantile)
                    & (transaction_frame["price_per_m2"] <= high_quantile)
                ].copy()
            )

        filtered = pd.concat(filtered_frames, ignore_index=True) if filtered_frames else frame.iloc[0:0].copy()

        group_cols = ["city", "neighborhood", "category", "transaction_type"]

        segment_stats = (
            filtered.groupby(group_cols, dropna=False)
            .agg(
                mean_price_per_m2=("price_per_m2", "mean"),
                median_price_per_m2=("price_per_m2", "median"),
                std_price_per_m2=("price_per_m2", "std"),
                min_price_per_m2=("price_per_m2", "min"),
                max_price_per_m2=("price_per_m2", "max"),
                listing_count=("external_id", "count"),
            )
            .reset_index()
        )

        segment_stats = segment_stats[segment_stats["listing_count"] >= min_neighborhood_size].copy()

        merged = filtered.merge(
            segment_stats[group_cols + ["mean_price_per_m2", "median_price_per_m2", "listing_count"]],
            on=group_cols,
            how="inner",
        )

        merged["segment_reference_price_per_m2"] = merged["median_price_per_m2"].where(
            merged["median_price_per_m2"].notna(),
            merged["mean_price_per_m2"],
        )

        threshold_factor = max(0.0, 1.0 - discount_threshold)
        merged["opportunity_threshold"] = merged["segment_reference_price_per_m2"] * threshold_factor
        merged["discount_vs_segment_mean_pct"] = (
            1.0 - (merged["price_per_m2"] / merged["segment_reference_price_per_m2"])
        ) * 100.0
        merged["premium_vs_segment_pct"] = (
            (merged["price_per_m2"] / merged["segment_reference_price_per_m2"]) - 1.0
        ) * 100.0

        merged["opportunity_score"] = (
            (1.0 - (merged["price_per_m2"] / merged["segment_reference_price_per_m2"])) * 100.0
        ).clip(lower=0.0, upper=100.0)

        merged["confidence_factor"] = (merged["listing_count"] / 10.0).clip(lower=0.0, upper=1.0)
        merged["investment_rank_score"] = (merged["opportunity_score"] * merged["confidence_factor"]).clip(
            lower=0.0,
            upper=100.0,
        )

        merged["is_opportunity"] = merged["opportunity_score"] >= (discount_threshold * 100.0)
        merged["is_overpriced"] = merged["premium_vs_segment_pct"] >= (discount_threshold * 100.0)
        merged["valuation_status"] = "neutro"
        merged.loc[merged["is_opportunity"], "valuation_status"] = "oportunidade"
        merged.loc[merged["is_overpriced"], "valuation_status"] = "inflacionado"

        # Rental yield estimation: compare sale listing price with rent segment mean for same city+bairro+categoria.
        rent_stats = segment_stats[segment_stats["transaction_type"] == "locacao"].copy()
        rent_stats = rent_stats.rename(columns={"mean_price_per_m2": "rent_mean_price_per_m2"})

        merged = merged.merge(
            rent_stats[["city", "neighborhood", "category", "rent_mean_price_per_m2"]],
            on=["city", "neighborhood", "category"],
            how="left",
        )
        merged["estimated_annual_yield_pct"] = None
        sale_mask = merged["transaction_type"] == "venda"
        merged.loc[sale_mask, "estimated_annual_yield_pct"] = (
            (
                (merged.loc[sale_mask, "rent_mean_price_per_m2"] * merged.loc[sale_mask, "area_total_m2"]) * 12.0
            )
            / merged.loc[sale_mask, "price"]
        ) * 100.0

        ranked = merged.sort_values(
            by=["investment_rank_score", "opportunity_score", "estimated_annual_yield_pct", "price_per_m2"],
            ascending=[False, False, False, True],
            na_position="last",
        )

        segment_stats_records: list[dict[str, Any]] = []
        for row in segment_stats.to_dict("records"):
            transaction_type = str(row.get("transaction_type") or "venda")
            segment_stats_records.append(
                {
                    "city": str(row.get("city") or ""),
                    "neighborhood": str(row.get("neighborhood") or ""),
                    "category": str(row.get("category") or ""),
                    "transaction_type": transaction_type,
                    "transaction_label": _transaction_label(transaction_type),
                    "listing_count": int(row.get("listing_count") or 0),
                    "mean_price_per_m2": _round_or_none(row.get("mean_price_per_m2")),
                    "median_price_per_m2": _round_or_none(row.get("median_price_per_m2")),
                    "std_price_per_m2": _round_or_none(row.get("std_price_per_m2")),
                    "min_price_per_m2": _round_or_none(row.get("min_price_per_m2")),
                    "max_price_per_m2": _round_or_none(row.get("max_price_per_m2")),
                }
            )

        analyzed_records: list[dict[str, Any]] = []
        for row in ranked.to_dict("records"):
            image_urls = row.get("Image_url") or []
            if not isinstance(image_urls, list):
                image_urls = []
            transaction_type = str(row.get("transaction_type") or "venda")
            analyzed_records.append(
                {
                    "source_agency": str(row.get("source_agency") or ""),
                    "external_id": str(row.get("external_id") or ""),
                    "title": str(row.get("title") or ""),
                    "category": str(row.get("category") or ""),
                    "city": str(row.get("city") or ""),
                    "neighborhood": str(row.get("neighborhood") or ""),
                    "transaction_type": transaction_type,
                    "transaction_label": _transaction_label(transaction_type),
                    "price": _safe_float(row.get("price")),
                    "area_total_m2": _safe_float(row.get("area_total_m2")),
                    "price_per_m2": _round_or_none(row.get("price_per_m2")),
                    "segment_mean_price_per_m2": _round_or_none(row.get("mean_price_per_m2")),
                    "segment_median_price_per_m2": _round_or_none(row.get("median_price_per_m2")),
                    "segment_reference_price_per_m2": _round_or_none(row.get("segment_reference_price_per_m2")),
                    "rental_mean_price_per_m2": _round_or_none(row.get("rent_mean_price_per_m2")),
                    "opportunity_threshold_price_per_m2": _round_or_none(row.get("opportunity_threshold")),
                    "discount_vs_segment_mean_pct": _round_or_none(row.get("discount_vs_segment_mean_pct")),
                    "discount_vs_neighborhood_mean_pct": _round_or_none(row.get("discount_vs_segment_mean_pct")),
                    "premium_vs_segment_pct": _round_or_none(row.get("premium_vs_segment_pct")),
                    "opportunity_score": _round_or_none(row.get("opportunity_score")),
                    "confidence_factor": _round_or_none(row.get("confidence_factor"), digits=4),
                    "investment_rank_score": _round_or_none(row.get("investment_rank_score")),
                    "is_opportunity": bool(row.get("is_opportunity")),
                    "is_overpriced": bool(row.get("is_overpriced")),
                    "valuation_status": str(row.get("valuation_status") or "neutro"),
                    "estimated_annual_yield_pct": _round_or_none(row.get("estimated_annual_yield_pct")),
                    "listing_count_in_segment": int(row.get("listing_count") or 0),
                    "image_url": str(image_urls[0]) if image_urls else "",
                    "image_urls_count": len(image_urls),
                    "url": str(row.get("url") or ""),
                    "captured_at": row.get("captured_at"),
                }
            )

        opportunity_records = [item for item in analyzed_records if item.get("valuation_status") == "oportunidade"]
        inflated_records = [item for item in analyzed_records if item.get("valuation_status") == "inflacionado"]

        sale_opportunities = [item for item in opportunity_records if item["transaction_type"] == "venda"]
        rental_opportunities = [item for item in opportunity_records if item["transaction_type"] == "locacao"]
        yield_covered_sales = [item for item in sale_opportunities if item.get("estimated_annual_yield_pct") is not None]

        result_document = {
            "generated_at": datetime.now(timezone.utc),
            "config": {
                "discount_threshold": discount_threshold,
                "min_neighborhood_size": min_neighborhood_size,
                "outlier_quantiles": [0.01, 0.99],
                "group_cols": group_cols,
            },
            "source_snapshot": listings_snapshot,
            "summary": {
                "total_input_properties": len(properties),
                "valid_after_basic_cleaning": int(len(frame)),
                "valid_after_outlier_filter": int(len(filtered)),
                "segments_analyzed": int(len(segment_stats)),
                "total_analyzed_properties": int(len(analyzed_records)),
                "opportunities_found": int(len(opportunity_records)),
                "inflated_found": int(len(inflated_records)),
                "sale_opportunities_found": int(len(sale_opportunities)),
                "rental_opportunities_found": int(len(rental_opportunities)),
                "yield_covered_sales": int(len(yield_covered_sales)),
            },
            "segment_stats": segment_stats_records,
            # backward-compatible alias for older consumers
            "neighborhood_stats": segment_stats_records,
            "analyzed_properties": analyzed_records,
            "opportunities": opportunity_records,
        }

        insert_result = await database["analysis_results"].insert_one(result_document)
        return {"analysis_id": str(insert_result.inserted_id), **result_document["summary"]}


async def run_market_analysis_if_needed(
    *,
    discount_threshold: float = 0.20,
    min_neighborhood_size: int = 3,
    database: AsyncIOMotorDatabase | None = None,
) -> dict[str, Any] | None:
    if database is None:
        database = get_database()

    current_snapshot = await _get_listings_snapshot(database)
    latest = await database["analysis_results"].find_one(
        sort=[("generated_at", -1), ("_id", -1)],
        projection={"source_snapshot": 1, "generated_at": 1},
    )

    if latest and latest.get("source_snapshot") == current_snapshot:
        return None

    return await run_market_analysis(
        discount_threshold=discount_threshold,
        min_neighborhood_size=min_neighborhood_size,
        database=database,
    )


async def analysis_auto_refresh_loop(
    *,
    interval_seconds: int = 180,
    discount_threshold: float = 0.20,
    min_neighborhood_size: int = 3,
    database: AsyncIOMotorDatabase | None = None,
) -> None:
    if database is None:
        database = get_database()

    while True:
        try:
            result = await run_market_analysis_if_needed(
                discount_threshold=discount_threshold,
                min_neighborhood_size=min_neighborhood_size,
                database=database,
            )
            if result is not None:
                print(
                    "[analysis-auto] analise atualizada "
                    f"(oportunidades={result.get('opportunities_found', 0)})"
                )
        except Exception as exc:
            print(f"[analysis-auto] falha ao atualizar analise: {exc}")

        await asyncio.sleep(max(30, interval_seconds))


async def get_latest_analysis(database: AsyncIOMotorDatabase | None = None) -> dict[str, Any] | None:
    if database is None:
        database = get_database()
    latest = await database["analysis_results"].find_one(sort=[("generated_at", -1), ("_id", -1)])
    if not latest:
        return None
    return _serialize_for_json(latest)


async def get_latest_opportunities(
    *,
    limit: int = 50,
    database: AsyncIOMotorDatabase | None = None,
) -> list[dict[str, Any]]:
    latest = await get_latest_analysis(database=database)
    if not latest:
        return []

    opportunities = latest.get("opportunities", [])
    if not isinstance(opportunities, list):
        return []

    bounded_limit = max(1, min(limit, 500))
    return opportunities[:bounded_limit]