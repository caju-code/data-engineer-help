import os
from http import HTTPStatus

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

from app.services.queimadas import listar_queimadas

load_dotenv()

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
API_PORT = int(os.getenv("API_PORT", "8000"))

app = FastAPI(title="API de Queimadas", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/queimadas")
@app.get("/queimadas/{uf}")
def get_queimadas(
    uf: str | None = None,
    ano: int | None = Query(default=None, description="Ano para filtrar"),
    limit: int = Query(
        default=DEFAULT_LIMIT, ge=1, le=10000, description="Limite de registros"
    ),
):
    try:
        data = listar_queimadas(uf=uf, ano=ano, limit=limit)
        return data
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=API_PORT, reload=True)
