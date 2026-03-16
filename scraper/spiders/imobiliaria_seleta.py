import os
import re
from collections.abc import Awaitable, Callable
from urllib.parse import urlparse

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from scraper.core.base import BaseSpider
from scraper.core.browser import managed_browser
from scraper.core.storage import upload_listing_images
from scraper.core.utils import parse_area, parse_price
from shared.models import PropertyModel, ScrapeBatchResult


class ImobiliariaSeletaSpider(BaseSpider):
    source_name = "imobiliaria_seleta"
    base_url = "https://imobiliariaseleta.com.br"

    def __init__(
        self,
        on_property_collected: Callable[[PropertyModel], Awaitable[None]] | None = None,
    ) -> None:
        purposes = os.getenv("SELETA_PURPOSES", "venda,locacao")
        self.purposes = [purpose.strip() for purpose in purposes.split(",") if purpose.strip()]
        # 0 means unlimited pages until an empty/repeated page is reached.
        self.max_pages = int(os.getenv("SELETA_MAX_PAGES", "0"))
        # 0 means do not cap the number of listings extracted from each page.
        self.max_listings_per_page = int(os.getenv("SELETA_MAX_LISTINGS_PER_PAGE", "0"))
        self.timeout_ms = int(os.getenv("SELETA_TIMEOUT_MS", "45000"))
        self.on_property_collected = on_property_collected

    def build_filter_url(self, purpose: str, page_number: int) -> str:
        return (
            f"{self.base_url}/filtro/{purpose}/todos/todas/todos/todos/todos/todos/todos/{page_number}"
        )

    async def scrape(self) -> ScrapeBatchResult:
        properties: list[PropertyModel] = []
        seen_urls: set[str] = set()
        seen_external_ids: set[str] = set()

        async with managed_browser() as browser:
            listing_page = await browser.new_page()
            detail_page = await browser.new_page()

            try:
                for purpose in self.purposes:
                    page_number = 1
                    detected_total_pages: int | None = None
                    while True:
                        if self.max_pages > 0 and page_number > self.max_pages:
                            break
                        if detected_total_pages is not None and page_number > detected_total_pages:
                            break

                        filter_url = self.build_filter_url(purpose, page_number)
                        print(f"Coletando rota {filter_url}")
                        listing_urls = await self.extract_listing_urls(listing_page, filter_url, purpose)

                        if page_number == 1:
                            detected_total_pages = await self.extract_total_pages(listing_page, purpose)
                            if detected_total_pages:
                                print(f"Total de paginas detectado para {purpose}: {detected_total_pages}")

                        if not listing_urls:
                            print(f"Sem resultados na pagina {page_number} ({purpose}); encerrando paginacao")
                            break

                        new_urls_on_page = 0

                        for listing_url in listing_urls:
                            if listing_url in seen_urls:
                                continue

                            external_id = self.extract_external_id(listing_url)
                            if external_id and external_id in seen_external_ids:
                                continue

                            new_urls_on_page += 1
                            seen_urls.add(listing_url)
                            if external_id:
                                seen_external_ids.add(external_id)
                            property_data = await self.extract_property(detail_page, listing_url)
                            if property_data is None:
                                continue

                            properties.append(property_data)
                            if self.on_property_collected is not None:
                                await self.on_property_collected(property_data)
                            print(f"Imovel validado: {property_data.title}")

                        if new_urls_on_page == 0:
                            print(f"Pagina {page_number} ({purpose}) repetida; encerrando paginacao")
                            break

                        page_number += 1
            finally:
                await listing_page.close()
                await detail_page.close()

        return ScrapeBatchResult(
            source=self.source_name,
            total_items=len(properties),
            items=properties,
        )

    async def extract_listing_urls(self, page: Page, filter_url: str, purpose: str) -> list[str]:
        try:
            await page.goto(filter_url, wait_until="networkidle", timeout=self.timeout_ms)
        except PlaywrightTimeoutError:
            print(f"Timeout ao abrir rota {filter_url}")
            return []

        raw_urls = await page.locator("a[href*='/imovel/']").evaluate_all(
            "elements => elements.map(element => element.href)"
        )

        valid_prefixes = {
            f"{self.base_url}/imovel/{purpose}/",
            f"{self.base_url}/imovel/venda-e-locacao/",
        }

        listing_urls: list[str] = []
        seen_ids: set[str] = set()
        for raw_url in raw_urls:
            normalized_url = raw_url.split("?", 1)[0].strip()
            if not normalized_url or not any(normalized_url.startswith(prefix) for prefix in valid_prefixes):
                continue
            external_id = self.extract_external_id(normalized_url)
            if not external_id or external_id in seen_ids:
                continue
            seen_ids.add(external_id)
            if normalized_url in listing_urls:
                continue
            listing_urls.append(normalized_url)

        if self.max_listings_per_page > 0:
            return listing_urls[: self.max_listings_per_page]
        return listing_urls

    async def extract_property(self, page: Page, listing_url: str) -> PropertyModel | None:
        try:
            try:
                await page.goto(listing_url, wait_until="networkidle", timeout=self.timeout_ms)
            except PlaywrightTimeoutError:
                print(f"Timeout ao abrir detalhe {listing_url}")
                return None

            title = await self.safe_inner_text(page, "h1.listing-page-title")
            price_text = await self.safe_inner_text(page, ".property-price.listing-page")
            description = await self.safe_inner_text(page, ".text-block-7")
            html = await page.content()

            source_image_urls = await page.locator("a[data-fancybox^='gallery']").evaluate_all(
                "elements => elements.map(element => element.href)"
            )
            source_image_urls = self.unique_urls(source_image_urls)

            external_id = self.extract_external_id(listing_url)
            if not external_id:
                print(f"Nao foi possivel obter external_id de {listing_url}")
                return None

            uploaded_image_urls = await upload_listing_images(
                source_agency="Imobiliaria Seleta",
                external_id=external_id,
                image_urls=source_image_urls,
            )

            category, city, neighborhood = self.parse_url_metadata(listing_url)
            suites = await self.extract_feature_number(page, "Suites")
            bedrooms = (
                await self.extract_feature_number(page, "Quartos")
                or await self.extract_feature_number(page, "Dormitorios")
                or suites
            )
            bathrooms = await self.extract_feature_number(page, "Banheiros")
            parking_spots = await self.extract_feature_number(page, "Garagem")
            area_util_m2 = self.extract_area_by_label(
                html,
                ["Área Privativa", "Area Privativa", "Área Útil", "Area Util", "Área Util"],
            )
            area_total_m2 = self.extract_area_by_label(
                html,
                ["Área Total", "Area Total", "Área Terreno", "Area Terreno", "Á. total"],
            ) or area_util_m2

            if area_total_m2 <= 0:
                area_total_m2 = self.extract_area_fallback(html, title)
            if area_util_m2 <= 0:
                area_util_m2 = None

            return PropertyModel(
                source_agency="Imobiliaria Seleta",
                external_id=external_id,
                title=self.clean_title(title),
                description=description or None,
                category=category,
                city=city,
                neighborhood=neighborhood,
                price=parse_price(price_text),
                area_total_m2=area_total_m2,
                area_util_m2=area_util_m2,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                parking_spots=parking_spots,
                url=listing_url,
                source_image_urls=source_image_urls,
                image_urls=uploaded_image_urls,
            )
        except Exception as exc:
            print(f"Erro ao validar imovel {listing_url}: {exc}")
            return None

    async def extract_total_pages(self, page: Page, purpose: str) -> int | None:
        hrefs = await page.locator(".container.paginacao a[href*='/filtro/']").evaluate_all(
            "elements => elements.map(element => element.href)"
        )
        max_page = 0
        pattern = re.compile(rf"/filtro/{purpose}/todos/todas/todos/todos/todos/todos/todos/(\d+)$")
        for href in hrefs:
            normalized = href.split("?", 1)[0].strip()
            match = pattern.search(normalized)
            if match:
                max_page = max(max_page, int(match.group(1)))
        return max_page or None

    async def safe_inner_text(self, page: Page, selector: str) -> str:
        locator = page.locator(selector).first
        if await locator.count() == 0:
            return ""
        value = await locator.inner_text()
        return value.strip()

    async def extract_feature_number(self, page: Page, label: str) -> int:
        script = """
        (blocks, desiredLabel) => {
          const normalizedDesired = desiredLabel.toLowerCase();
          for (const block of blocks) {
            const labelElement = block.querySelector('.listing-feature-title');
            const numberElement = block.querySelector('.listing-feature-number');
            if (!labelElement || !numberElement) {
              continue;
            }

            const blockLabel = labelElement.textContent
              .normalize('NFD')
              .replace(/[\u0300-\u036f]/g, '')
              .trim()
              .toLowerCase();
            if (blockLabel === normalizedDesired.toLowerCase()) {
              return numberElement.textContent.trim();
            }
          }

          return '';
        }
        """
        raw_value = await page.locator(".listing-feature-block").evaluate_all(script, label)
        digits = re.sub(r"\D", "", raw_value or "")
        return int(digits) if digits else 0

    def extract_external_id(self, listing_url: str) -> str:
        match = re.search(r"/(\d+)$", listing_url)
        return match.group(1) if match else ""

    def parse_url_metadata(self, listing_url: str) -> tuple[str, str, str]:
        segments = [segment for segment in urlparse(listing_url).path.split("/") if segment]
        category_slug = segments[2] if len(segments) > 2 else "imovel"
        city_state_slug = segments[3] if len(segments) > 3 else "cascavel-pr"
        neighborhood_slug = segments[4] if len(segments) > 4 else "centro"

        city_slug = city_state_slug.rsplit("-", 1)[0]
        category = self.slug_to_title(category_slug)
        city = self.slug_to_title(city_slug)
        neighborhood = self.slug_to_title(neighborhood_slug)
        return category, city, neighborhood

    def slug_to_title(self, value: str) -> str:
        cleaned = value.replace("--", "-")
        return " ".join(part.capitalize() for part in cleaned.split("-") if part)

    def clean_title(self, title: str) -> str:
        cleaned = re.sub(r"\s*Ref\.:\s*\d+", "", title or "")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned or "Imovel sem titulo"

    def extract_area_by_label(self, html: str, labels: list[str]) -> float:
        for label in labels:
            pattern = rf"<b>([\d\.,]+)m²</b>\s*\(m²\)\s*{label}"
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                parsed = parse_area(match.group(1))
                if parsed > 0:
                    return parsed

        fallback = re.search(r"([\d\.,]+)m²", html, re.IGNORECASE)
        if fallback:
            return parse_area(fallback.group(1))
        return 0.0

    def extract_area_fallback(self, html: str, title: str) -> float:
        # Try generic m2 patterns commonly found in free text.
        for match in re.findall(r"([\d\.,]+)\s*m(?:²|2)\b", html + " " + title, re.IGNORECASE):
            parsed = parse_area(match)
            if parsed > 0:
                return parsed

        # Convert hectares to square meters when present.
        for match in re.findall(r"([\d\.,]+)\s*ha\b", html + " " + title, re.IGNORECASE):
            hectares = parse_area(match)
            if hectares > 0:
                return hectares * 10000

        # Convert alqueire/alq to square meters (approx. alqueire paulista).
        for match in re.findall(r"([\d\.,]+)\s*(?:alqueires?|alq)\b", html + " " + title, re.IGNORECASE):
            alqueires = parse_area(match)
            if alqueires > 0:
                return alqueires * 24200

        # Keep listing ingest resilient even when source does not expose area.
        return 1.0

    def unique_urls(self, urls: list[str]) -> list[str]:
        unique: list[str] = []
        for url in urls:
            cleaned = url.split("?", 1)[0].strip()
            if cleaned and cleaned not in unique:
                unique.append(cleaned)
        return unique
