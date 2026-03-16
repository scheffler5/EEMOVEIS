# S3 Local

Esta pasta documenta o armazenamento S3 local do projeto usando MinIO.

- Servico: `minio`
- Console: `http://localhost:9001`
- API S3: `http://localhost:9000`
- Bucket inicial: `images`

As imagens baixadas pelos scrapers devem ser enviadas para o bucket `images` e o link publico resultante deve ser salvo no MongoDB no campo `Image_url`.