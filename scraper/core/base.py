from abc import ABC, abstractmethod

from shared.models import ScrapeBatchResult


class BaseSpider(ABC):
    source_name: str

    @abstractmethod
    async def scrape(self) -> ScrapeBatchResult:
        raise NotImplementedError
