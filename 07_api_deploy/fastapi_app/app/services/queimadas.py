from app.db import execute_query

TABLE_NAME = "workspace.gold.queimadas_uf"
DEFAULT_LIMIT = 100


def listar_queimadas(uf: str | None = None, ano: int | None = None, limit: int = DEFAULT_LIMIT) -> list[dict]:
    query = f"""
        SELECT sigla_uf, bioma, mes, ano, total_focos
        FROM {TABLE_NAME}
    """
    params: dict = {"limit": limit}

    if uf:
        query += " WHERE sigla_uf = :uf"
        params["uf"] = uf.upper()

    if ano:
        query += " AND ano = :ano" if uf else " WHERE ano = :ano"
        params["ano"] = ano

    query += " ORDER BY total_focos DESC LIMIT :limit"

    return execute_query(query, params)
