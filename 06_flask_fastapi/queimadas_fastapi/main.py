import os

import uvicorn
from databricks import sql
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
API_PORT = int(os.getenv("API_PORT", "8000"))
TABLE_NAME = "workspace.gold.queimadas_uf"


def execute_query(
    uf: str | None = None, year: int | None = None, limit: int = 100
) -> list[dict]:
    query = f"""
    SELECT sigla_uf, bioma, mes, ano, total_focos
    FROM {TABLE_NAME}
    """

    params: dict = {"limit": limit}
    if uf:
        query += " WHERE sigla_uf = :uf"
        params["uf"] = uf.upper()

    if year:
        query += " WHERE ano = :year" if not uf else " AND ano = :year"
        params["year"] = year

    query += " ORDER BY total_focos DESC LIMIT :limit"

    print(query)
    print(params)

    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            df = cursor.fetchall_arrow().to_pandas()
            return df.to_dict(orient="records")


app = FastAPI()


@app.get("/queimadas")
@app.get("/queimadas/{uf}")
def get_queimadas(
    uf: str | None = None,
    year: int | None = Query(default=None, description="Ano para filtrar"),
    limit: int = Query(
        default=DEFAULT_LIMIT, ge=1, le=10000, description="Limite de registros"
    ),
):
    try:
        return execute_query(uf=uf, year=year, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=API_PORT, reload=True)
