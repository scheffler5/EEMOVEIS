import math

import pandas as pd

from shared.models import PriceInsight, PropertyModel


def build_price_insights(listings: list[PropertyModel]) -> list[PriceInsight]:
    if not listings:
        return []

    frame = pd.DataFrame([listing.model_dump() for listing in listings])
    if frame.empty:
        return []

    frame["price_per_m2"] = frame.apply(
        lambda row: row["price"] / row["area_total_m2"]
        if row.get("area_total_m2") and row["area_total_m2"] > 0
        else math.nan,
        axis=1,
    )

    grouped = frame.groupby(["source_agency", "city"], dropna=False).agg(
        average_price=("price", "mean"),
        average_price_per_m2=("price_per_m2", "mean"),
        total_listings=("external_id", "count"),
    )

    insights: list[PriceInsight] = []
    for (source_agency, city), row in grouped.iterrows():
        insights.append(
            PriceInsight(
                source_agency=source_agency,
                city=city,
                average_price=round(float(row["average_price"]), 2),
                average_price_per_m2=None
                if pd.isna(row["average_price_per_m2"])
                else round(float(row["average_price_per_m2"]), 2),
                total_listings=int(row["total_listings"]),
            )
        )

    return insights
