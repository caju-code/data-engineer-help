# API Flask — Consulta de Queimadas

API simples em Flask para consultar focos de queimadas por UF no Databricks SQL Warehouse, pronta para deploy no Render.

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/health` | Health check |
| GET | `/queimadas` | Lista todas as UFs |
| GET | `/queimadas/<sigla_uf>` | Filtra por UF |
| GET | `/queimadas/<sigla_uf>?ano=2025` | Filtra por UF e ano |

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
uv run python app.py
```

A API sobe em `http://localhost:5000`.

### Testar

```bash
curl http://localhost:5000/health
curl "http://localhost:5000/queimadas/MT?ano=2025"
```

---

## Deploy no Render

### 1. Conecte o repositório

No [Render Dashboard](https://dashboard.render.com), clique em **New + > Web Service** e conecte seu repositório GitHub.

### 2. Configuração do Web Service

| Campo | Valor |
|-------|-------|
| **Runtime** | Python 3 |
| **Build Command** | `pip install uv && uv sync --frozen` |
| **Start Command** | `.venv/bin/gunicorn app:app` |

### 3. Variáveis de ambiente

Adicione no Render:

| Variável | Valor |
|----------|-------|
| `SERVER_HOSTNAME` | `dbc-***.cloud.databricks.com` |
| `HTTP_PATH` | `/sql/1.0/warehouses/***` |
| `ACCESS_TOKEN` | `dapi****` |
| `DEFAULT_LIMIT` | `100` |

### 4. Deploy

Clique em **Deploy**. O Render vai:

1. Instalar o uv
2. Rodar `uv sync --frozen` para instalar as dependências exatas do `uv.lock`
3. Iniciar a API com gunicorn

### 5. Testar a URL pública

```bash
curl https://sua-api.onrender.com/health
curl "https://sua-api.onrender.com/queimadas/MT?ano=2025"
```

---

## Estrutura do projeto

```
flask_app/
  app.py           # rotas e entrypoint
  db.py            # conexao com Databricks
  pyproject.toml   # dependencias do projeto
  uv.lock          # lockfile (gerado pelo uv sync)
  .env.example     # template de variaveis de ambiente
  .python-version  # versao do Python
  README.md        # documentacao
```
