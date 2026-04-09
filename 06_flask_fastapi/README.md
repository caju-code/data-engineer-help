# Flask e FastAPI

Comparacao entre duas APIs que consultam dados de queimadas do Databricks.

## Subpastas

### `queimadas_flask/`
Implementacao com **Flask**. Contem:
- `app_all_in_one.py` - aplicacao em um único arquivo -> **a que usamos na aula**
- `full_app/` - estrutura modular -> apenas para referência, se assemelhando mais ao que encontramos no "mundo real", com melhores práticas de eng. de software
- Configuracao via `.env` -> `.env.example` deve ser base para criação do `.env`

### `queimadas_fastapi/`
Implementacao com **FastAPI**. Contem:
- `main.py` - aplicacao principal com rotas e documentacao automatica via Swagger
- Configuracao via `.env` -> `.env.example` deve ser base para criação do `.env`

## O que as APIs fazem

Ambas consultam a tabela `workspace.gold.queimadas_uf` (focos de queimadas por UF, bioma, mes e ano) e retornam os dados como JSON.

## Como executar

### Instalacao do uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Flask

```bash
cd queimadas_flask
cp .env.example .env  # preencha as variaveis de ambiente
uv sync
uv run python app_all_in_one.py
```

### FastAPI

```bash
cd queimadas_fastapi
cp .env.example .env  # preencha as variaveis de ambiente
uv sync
uv run python main.py
```

Acesse a documentacao Swagger em `http://localhost:8000/docs`.
