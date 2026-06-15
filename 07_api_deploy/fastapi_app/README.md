# API FastAPI — Consulta de Queimadas

API em FastAPI para consultar focos de queimadas por UF no Databricks SQL Warehouse, publicada na AWS com Docker + Amazon ECR + Amazon ECS Express Mode.

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/health` | Health check |
| GET | `/queimadas` | Lista todas as UFs |
| GET | `/queimadas/{sigla_uf}` | Filtra por UF |
| GET | `/queimadas/{sigla_uf}?ano=2025` | Filtra por UF e ano |

### Exemplo de resposta

```json
[
  {
    "sigla_uf": "MT",
    "bioma": "Amazônia",
    "mes": 8,
    "ano": 2025,
    "total_focos": 12543
  }
]
```

## Estrutura do projeto

```
fastapi_app/
  app/
    __init__.py
    main.py              # FastAPI, rotas e entrypoint
    db.py                # conexao com Databricks
    services/
      __init__.py
      queimadas.py       # logica de consulta
  pyproject.toml         # dependencias do projeto
  uv.lock                # lockfile (gerado pelo uv sync)
  Dockerfile             # imagem Docker com uv
  .dockerignore
  .env.example           # template de variaveis de ambiente
  README.md              # documentacao
```

## Dependências

Gerenciadas com **uv** via `pyproject.toml` + `uv.lock` (sem `requirements.txt`).

## Como rodar localmente

### 1. Instalar o uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Preencha as credenciais do Databricks no `.env`:

```
SERVER_HOSTNAME="dbc-***.cloud.databricks.com"
HTTP_PATH="/sql/1.0/warehouses/***"
ACCESS_TOKEN="dapi****"
```

### 3. Sincronizar dependências e executar

```bash
uv sync
uv run python -m app.main
```

A API sobe em `http://localhost:8000`.

Acesse a documentação automática em `http://localhost:8000/docs`.

### Testar com curl

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/queimadas/MT?ano=2025"
```

---

## Docker

### Build da imagem

```bash
docker build -t fastapi-queimadas .
```

### Executar localmente com Docker

```bash
docker run -p 8000:8000 --env-file .env fastapi-queimadas
```

### Como o Dockerfile funciona

```dockerfile
FROM python:3.11-slim
RUN pip install uv --no-cache-dir     # instala uv no container
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev         # instala deps exatas do lockfile
COPY . .
CMD [".venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- `uv sync --frozen` usa o lockfile para instalar versões exatas, sem resolver novamente.
- `--no-dev` pula dependências de desenvolvimento.
- O entrypoint usa o uvicorn instalado dentro do `.venv` do projeto.

---

## Deploy na AWS com ECS Express Mode

### 1. Criar repositório no Amazon ECR

```bash
aws ecr create-repository --repository-name fastapi-queimadas
```

Anote o URI do repositório (ex: `<account-id>.dkr.ecr.us-east-1.amazonaws.com/fastapi-queimadas`).

### 2. Autenticar e fazer push da imagem

```bash
# Login no ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  <account-id>.dkr.ecr.us-east-1.amazonaws.com

# Tag da imagem
docker tag fastapi-queimadas:latest \
  <account-id>.dkr.ecr.us-east-1.amazonaws.com/fastapi-queimadas:latest

# Push
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/fastapi-queimadas:latest
```

### 3. Criar as roles IAM obrigatórias

O ECS Express Mode exige duas roles IAM:

#### Task Execution Role

Permite que o ECS baixe a imagem do ECR e envie logs para o CloudWatch.

```bash
aws iam create-role \
  --role-name ecsTaskExecutionRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ecs-tasks.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam attach-role-policy \
  --role-name ecsTaskExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
```

#### Infrastructure Role

Permite que o ECS Express Mode crie recursos de infraestrutura (ALB, rede, scaling) em seu nome.

```bash
aws iam create-role \
  --role-name ecsInfrastructureRoleForExpressServices \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "express-gateway.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam attach-role-policy \
  --role-name ecsInfrastructureRoleForExpressServices \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRoleforExpressGatewayServices
```

### 4. Criar o serviço ECS Express Mode

Substitua os ARNs pelos ARNs reais das suas roles.

```bash
aws ecs create-express-gateway-service \
  --execution-role-arn arn:aws:iam::<account-id>:role/ecsTaskExecutionRole \
  --infrastructure-role-arn arn:aws:iam::<account-id>:role/ecsInfrastructureRoleForExpressServices
```

### 5. Configurar variáveis de ambiente no serviço

No console da AWS, adicione as mesmas variáveis do `.env` ao serviço criado:

- `SERVER_HOSTNAME`
- `HTTP_PATH`
- `ACCESS_TOKEN`
- `DEFAULT_LIMIT`

### 6. Testar a URL pública

```bash
curl https://url-do-seu-servico/health
curl "https://url-do-seu-servico/queimadas/MT?ano=2025"
```

Acesse também `https://url-do-seu-servico/docs` para o Swagger.

---

## O que o ECS Express Mode faz por trás dos panos

- Cria um **ECS Service** rodando em **Fargate** (serverless containers)
- Provisiona um **Application Load Balancer (ALB)** com HTTPS
- Configura **networking** (VPC, subnets, security groups)
- Habilita **logs** no CloudWatch
- Configura **health checks** e **auto scaling**
- Tudo isso sem você precisar gerenciar servidores ou clusters manualmente

---

## O que ajustar antes do deploy real

- **account-id**: substituir pelo ID real da conta AWS nos comandos e ARNs
- **Região AWS**: `us-east-1` pode ser alterada conforme necessidade
- **Credenciais Databricks**: preencher valores reais no serviço ECS
- **Variáveis de ambiente sensíveis**: ACCESS_TOKEN deve ser tratado como segredo (AWS Secrets Manager é recomendado em produção)
- **Políticas IAM**: as roles acima usam policies gerenciadas da AWS; ambientes mais restritivos podem exigir policies customizadas
- **Porta no Dockerfile**: 8000, mas o ALB pode ser configurado para 443 com redirect
