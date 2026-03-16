# EEMoveis

Plataforma de monitoramento e analise imobiliaria com foco em descoberta de oportunidades e ativos inflacionados.

## Componentes

- `scraper`: coleta anuncios com Python + Playwright.
- `backend`: API analitica com FastAPI, Motor e Pandas.
- `frontend`: dashboard com FastAPI + Jinja2 + Chart.js + Leaflet.
- `mongo`: armazenamento dos lotes brutos e snapshots de analise.
- `minio`: armazenamento local estilo S3 para imagens.

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

## Como o Web Scraping Funciona

1. O scraper navega as paginas da imobiliaria com Playwright.
2. Para cada anuncio, extrai campos estruturados (titulo, bairro, cidade, preco, area, categoria, URL e imagens).
3. As imagens sao baixadas e publicadas no bucket (MinIO/S3), sem interromper o fluxo se alguma URL falhar.
4. Os registros sao persistidos incrementalmente no MongoDB em lotes para nao perder dados em ciclos longos.
5. Na persistencia existe deduplicacao por `(source_agency, external_id)` para evitar repeticao em reinicios.

## Calculos Matematicos da Analise

A analise roda por segmentos:

$$
s = (cidade, bairro, categoria, transaction\_type)
$$

onde `transaction_type` e separado em `venda` e `locacao`.

### 1) Preco por m² do imovel

$$
p_i = \frac{preco_i}{area_{m2,i}}
$$

### 2) Filtro de outliers

Para cada tipo de transacao, remove extremos de `price_per_m2` fora dos quantis de 1% e 99%:

$$
Q_{0.01} \le p_i \le Q_{0.99}
$$

### 3) Estatisticas do segmento

Para cada segmento `s`, calcula:

- media: $\mu_s$
- mediana: $\tilde{\mu}_s$
- quantidade: $n_s$

Somente segmentos com tamanho minimo entram no ranking (ex.: $n_s \ge 4$).

### 4) Referencia robusta

$$
r_s =
\begin{cases}
	ilde{\mu}_s, & \text{se mediana existir}\\
\mu_s, & \text{caso contrario}
\end{cases}
$$

### 5) Desconto e score de oportunidade

Com limiar de desconto $\delta$ (ex.: 15%):

$$
D_i = \left(1 - \frac{p_i}{r_s}\right) \cdot 100
$$

$$
S_i = clip(D_i, 0, 100)
$$

Um imovel e oportunidade quando:

$$
S_i \ge \delta \cdot 100
$$

### 6) Imovel inflacionado

Premium sobre a referencia:

$$
P_i = \left(\frac{p_i}{r_s} - 1\right) \cdot 100
$$

Um imovel e marcado como inflacionado quando:

$$
P_i \ge \delta \cdot 100
$$

### 7) Fator de confianca e rank final

$$
C_s = clip\left(\frac{n_s}{10}, 0, 1\right)
$$

$$
R_i = clip(S_i \cdot C_s, 0, 100)
$$

`R_i` e o score usado no leaderboard/ranking.

### 8) Yield estimado para venda

Para imoveis de venda, usando media de locacao do mesmo segmento geoespacial/categoria:

$$
Yield_{anual}(\%) =
\frac{(aluguel\_medio\_{m2} \cdot area_{m2} \cdot 12)}{preco\_venda} \cdot 100
$$

## Dashboard (Screenshot)

Imagem enviada do dashboard Brokers:

![Dashboard Brokers](docs/images/dashboard-brokers.png)

## Observacoes

- O scraper roda em loop com intervalo configuravel por `SCRAPER_INTERVAL_SECONDS`.
- A API de analise salva snapshots em `analysis_results`.
- O frontend permite filtros por finalidade, status de valoracao (oportunidade/inflacionado/geral), categoria e busca textual.
- O Mongo local persiste dados no volume `mongo_data`.
- O bucket `images` concentra imagens tratadas do scraping.

## Infraestrutura AWS

A pasta `infra/terraform` contem o README com proposta de arquitetura em nuvem e justificativas de custo/performance.
