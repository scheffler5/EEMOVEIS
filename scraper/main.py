import asyncio
import os
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient

from scraper.spiders.imobiliaria_seleta import ImobiliariaSeletaSpider
from shared.models import PropertyModel


def persist_batch(batch: dict) -> int:
    mongo_uri = os.getenv("MONGO_URI")
    database_name = os.getenv("MONGO_DATABASE", "eemoveis")
    if not mongo_uri:
        return 0

    client = MongoClient(mongo_uri)
    try:
        collection = client[database_name]["listings"]
        source = batch.get("source")
        items = batch.get("items", [])

        # Prevent duplicated listings after container restarts by skipping already ingested IDs.
        existing_ids = set(
            collection.distinct("items.external_id", {"source": source, "items.external_id": {"$ne": None}})
        )

        unique_batch_items: list[dict] = []
        seen_in_batch: set[str] = set()
        for item in items:
            external_id = item.get("external_id")
            if not external_id:
                continue
            if external_id in seen_in_batch or external_id in existing_ids:
                continue
            seen_in_batch.add(external_id)
            unique_batch_items.append(item)

        if not unique_batch_items:
            print(f"[{source}] nenhum novo imovel para persistir")
            return 0

        payload = {
            **batch,
            "items": unique_batch_items,
            "total_items": len(unique_batch_items),
            "ingested_at": datetime.now(timezone.utc),
        }
        collection.insert_one(payload)
        return len(unique_batch_items)
    finally:
        client.close()


class IncrementalBatchPersister:
    def __init__(self, source: str, flush_size: int = 25) -> None:
        self.source = source
        self.flush_size = max(1, flush_size)
        self.buffer: list[dict[str, Any]] = []
        self.persisted_total = 0

    async def add(self, property_data: PropertyModel) -> None:
        self.buffer.append(property_data.model_dump(mode="json", by_alias=True))
        if len(self.buffer) >= self.flush_size:
            self.flush()

    def flush(self) -> int:
        if not self.buffer:
            return 0
        payload = {
            "source": self.source,
            "total_items": len(self.buffer),
            "items": self.buffer,
        }
        persisted = persist_batch(payload)
        self.persisted_total += persisted
        print(f"[{self.source}] flush incremental: {persisted} persistidos")
        self.buffer = []
        return persisted


async def run_scraper():
    persister = IncrementalBatchPersister(source="imobiliaria_seleta", flush_size=25)
    spider = ImobiliariaSeletaSpider(on_property_collected=persister.add)
    batch = await spider.scrape()
    persister.flush()
    return batch, persister.persisted_total


async def run_once() -> None:
    batch, persisted = await run_scraper()
    print(f"[{batch.source}] coletados {batch.total_items} registros | persistidos {persisted}")


async def run_forever() -> None:
    interval = int(os.getenv("SCRAPER_INTERVAL_SECONDS", "1800"))
    while True:
        try:
            await run_once()
        except Exception as exc:
            print(f"[scraper] erro nao tratado no ciclo: {exc}")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(run_forever())
