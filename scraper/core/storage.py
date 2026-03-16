import mimetypes
import os
from io import BytesIO
from urllib.parse import urlparse

import httpx
from minio import Minio
from minio.error import S3Error


def get_storage_client() -> Minio | None:
    endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")

    if not endpoint or not access_key or not secret_key:
        return None

    secure = os.getenv("S3_SECURE", "false").lower() == "true"
    normalized_endpoint = endpoint.replace("http://", "").replace("https://", "")
    return Minio(normalized_endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def build_public_url(bucket_name: str, object_name: str) -> str:
    public_base_url = os.getenv("S3_PUBLIC_BASE_URL", "http://localhost:9000").rstrip("/")
    return f"{public_base_url}/{bucket_name}/{object_name}"


def guess_extension(content_type: str | None, image_url: str) -> str:
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if guessed:
            return guessed

    parsed = urlparse(image_url)
    _, extension = os.path.splitext(parsed.path)
    return extension or ".jpg"


async def upload_listing_images(source_agency: str, external_id: str, image_urls: list[str]) -> list[str]:
    bucket_name = os.getenv("S3_BUCKET", "images")
    client = get_storage_client()
    if not client or not image_urls:
        return []

    uploaded_urls: list[str] = []
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as http_client:
        for index, image_url in enumerate(image_urls, start=1):
            try:
                response = await http_client.get(image_url)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                print(f"Falha ao baixar imagem {image_url}: {exc}")
                continue

            content = response.content
            content_type = response.headers.get("content-type", "image/jpeg")
            extension = guess_extension(content_type, image_url)
            object_name = f"{source_agency.lower().replace(' ', '-')}/{external_id}/{index}{extension}"

            try:
                client.put_object(
                    bucket_name,
                    object_name,
                    BytesIO(content),
                    length=len(content),
                    content_type=content_type,
                )
            except S3Error as exc:
                print(f"Falha ao enviar imagem para o bucket: {exc}")
                continue

            uploaded_urls.append(build_public_url(bucket_name, object_name))

    return uploaded_urls