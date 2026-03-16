# EEMoveis

Base inicial para uma plataforma de monitoramento imobiliario com cinco componentes:

- `scraper`: aquisicao de anuncios com Python e Playwright.
- `backend`: API analitica com FastAPI, Motor e Pandas.
- `frontend`: interface server-side com FastAPI, Jinja2 e Tailwind via CDN.
- `mongo`: banco local para desenvolvimento orquestrado pelo Docker Compose.
- `minio`: bucket S3 local para armazenar imagens dos imoveis.

## Estrutura

```text
infra/
  terraform/
scraper/
backend/
frontend/
s3/
shared/
data/
```

## Subida local

1. Construa e suba os containers:

```bash
docker compose up --build
```

2. Acesse os servicos:

- Frontend: http://localhost:8080
- Backend: http://localhost:8000/health
- MongoDB: mongodb://localhost:27017
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001

## Observacoes

- O `scraper` roda continuamente em intervalo definido por `SCRAPER_INTERVAL_SECONDS`.
- Os spiders atuais sao placeholders e devem ser substituidos pela logica real de cada imobiliaria.
- O backend consolida os documentos da colecao `listings` e calcula medias por origem e cidade.
- O frontend consulta o backend e exibe um painel inicial simples.
- O Mongo local usa imagem custom em Alpine definida em `infra/docker/mongo/Dockerfile` e persiste dados no volume nomeado `mongo_data`.
- O MinIO cria automaticamente o bucket publico `images` para guardar as imagens baixadas pelos scrapers.
- Cada imovel pode ter varias imagens; o contrato salva as URLs originais em `source_image_urls` e as URLs do bucket no campo serializado `Image_url`.

## Infraestrutura AWS

A pasta `infra/terraform` contem uma base para provisionar:

- 1 bucket S3 para dados brutos.
- 1 instancia EC2 para hospedar a stack.
- 1 cluster DocumentDB para ambiente AWS.

Antes de aplicar o Terraform, ajuste valores como `subnet_ids`, `vpc_id`, `key_name`, CIDRs permitidos e a AMI conforme sua conta e regiao.
