from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class PropertyModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source_agency: str = Field(..., description="Nome da imobiliaria")
    external_id: str = Field(..., description="ID do imovel no site de origem")
    title: str
    description: Optional[str] = None
    category: str = Field(..., description="Casa, Apartamento, Terreno, etc.")
    city: str = "Cascavel"
    neighborhood: str
    price: float
    condo_fee: float = 0.0
    area_total_m2: float
    area_util_m2: Optional[float] = None
    bedrooms: int = 0
    bathrooms: int = 0
    parking_spots: int = 0
    url: HttpUrl
    source_image_urls: list[HttpUrl] = Field(
        default_factory=list,
        description="URLs originais encontradas durante o scraping",
    )
    image_urls: list[HttpUrl] = Field(
        default_factory=list,
        serialization_alias="Image_url",
        validation_alias="Image_url",
        description="URLs publicas das imagens enviadas ao bucket do projeto",
    )
    captured_at: datetime = Field(default_factory=datetime.now)

    @field_validator("price", "area_total_m2")
    @classmethod
    def must_be_positive(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("O valor deve ser positivo")
        return value


class ScrapeBatchResult(BaseModel):
    source: str
    collected_at: datetime = Field(default_factory=datetime.now)
    total_items: int
    items: list[PropertyModel]


class PriceInsight(BaseModel):
    source_agency: str
    city: str
    average_price: float
    average_price_per_m2: float | None = None
    total_listings: int
    generated_at: datetime = Field(default_factory=datetime.now)


PropertyListing = PropertyModel
