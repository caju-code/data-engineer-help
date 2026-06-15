# AGENTS.md — Data Engineer Help (Aula 7)

**Idioma: comentários e documentação deste repositório devem estar em português brasileiro. Código permanece em inglês.**

## Tipo de repositório
Repositório educacional de mentoria. Sem testes, CI ou linters/formatters configurados.

## Gerenciamento de dependências
Todos os projetos Python usam **uv** (não pip/requirements.txt). Dependências ficam em `pyproject.toml` + `uv.lock`.

```bash
uv sync          # instala deps do lockfile
uv sync --frozen # instala exatamente o que está no lockfile (usado no build de deploy)
uv run <cmd>     # executa comando no venv do projeto
```

## Estrutura do projeto

```
07_api_deploy/          ← Conteúdo da Aula 7 (foco em deploy, pode estar vazio)
06_flask_fastapi/       ← APIs usadas na Aula 7
  queimadas_flask/      → API Flask (deploy no Render)
    app_all_in_one.py   → entrypoint principal (versão arquivo único, usada em aula)
    full_app/           → versão modular (apenas referência)
  queimadas_fastapi/    → API FastAPI (deploy no ECS Express Mode)
    main.py             → entrypoint
01-05_*/                → Notebooks Databricks (não relevantes para Aula 7)
```

## Variáveis de ambiente (obrigatórias para execução local)
Ambas as APIs precisam de `.env` (copiar de `.env.example`):
- `DATABRICKS_SERVER_HOSTNAME` — servidor Databricks
- `DATABRICKS_HTTP_PATH` — caminho do SQL warehouse
- `DATABRICKS_ACCESS_TOKEN` — token de acesso pessoal
- `API_PORT` — padrão: Flask=5000, FastAPI=8000
- `DEFAULT_LIMIT` — padrão 100

## Execução local

```bash
# Flask
cd 06_flask_fastapi/queimadas_flask
cp .env.example .env   # preencha as credenciais
uv sync
uv run python app_all_in_one.py

# FastAPI
cd 06_flask_fastapi/queimadas_fastapi
cp .env.example .env
uv sync
uv run python main.py
```

Swagger do FastAPI em `http://localhost:8000/docs`.

## Banco de dados
Ambas as APIs consultam `workspace.gold.queimadas_uf` (databricks-sql-connector com pyarrow). Endpoint: `GET /queimadas[/{uf}]?year=&limit=`.

## Destinos de deploy (Aula 7)
- **Flask** → Render (Web Service, build: `pip install uv && uv sync --frozen`, start: `.venv/bin/gunicorn app:app` ou `uv run gunicorn app:app`)
- **FastAPI** → AWS ECS Express Mode (imagem container com Dockerfile, push para ECR, precisa de 2 roles IAM: `ecsTaskExecutionRole` + `ecsInfrastructureRoleForExpressServices`)

## Docker (deploy FastAPI)
Padrão de Dockerfile: `FROM python:3.11-slim` → `pip install uv` → `uv sync --frozen --no-dev` → `CMD [".venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]`
