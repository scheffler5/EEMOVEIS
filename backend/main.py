import asyncio
import os
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from backend.api.routes import router
from backend.db.mongo import get_database
from backend.services.analyzer import analysis_auto_refresh_loop


@asynccontextmanager
async def lifespan(_: FastAPI):
	task: asyncio.Task | None = None

	auto_enabled = os.getenv("ANALYSIS_AUTO_ENABLED", "true").lower() == "true"
	if auto_enabled:
		interval_seconds = int(os.getenv("ANALYSIS_AUTO_INTERVAL_SECONDS", "180"))
		discount_threshold = float(os.getenv("ANALYSIS_AUTO_DISCOUNT_THRESHOLD", "0.20"))
		min_neighborhood_size = int(os.getenv("ANALYSIS_AUTO_MIN_SEGMENT_SIZE", "3"))
		database = get_database()
		task = asyncio.create_task(
			analysis_auto_refresh_loop(
				interval_seconds=interval_seconds,
				discount_threshold=discount_threshold,
				min_neighborhood_size=min_neighborhood_size,
				database=database,
			)
		)

	try:
		yield
	finally:
		if task is not None:
			task.cancel()
			with suppress(asyncio.CancelledError):
				await task

app = FastAPI(title="EEMoveis Backend", version="0.1.0", lifespan=lifespan)
app.include_router(router)
