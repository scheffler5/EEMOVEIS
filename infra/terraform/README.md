# Terraform - Brokers Cloud Base

Infraestrutura modular para o projeto:
- VPC com subnets publicas
- S3 para imagens dos imoveis
- ECS Fargate com EventBridge para rodar scraper por agenda

## Estrutura

- main.tf: orquestra os modulos
- variables.tf: variaveis de entrada
- outputs.tf: saidas principais
- modules/network: VPC, subnet, IGW e rotas
- modules/s3_images: bucket de imagens + criptografia + lifecycle
- modules/ecs_scraper: cluster ECS, task Fargate e agendamento

## Pre-requisitos

1. Terraform >= 1.5
2. Credenciais AWS configuradas
3. Imagem do scraper publicada no ECR
4. MongoDB Atlas (ou EC2 Mongo) pronto

## Passos

1. Copie o arquivo de exemplo:

```bash
cp terraform.tfvars.example terraform.tfvars
```

2. Preencha variaveis sensiveis em terraform.tfvars.

3. Execute:

```bash
terraform init
terraform plan
terraform apply
```

## Observacoes

- O scraper e agendado no EventBridge e executa `run_once`.
- O bucket S3 pode ficar privado (recomendado) ou publico via `images_allow_public_read`.
- Para custo baixo, combine Atlas Free Tier + Fargate sob demanda.
