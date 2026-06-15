import os

from databricks import sql
from dotenv import load_dotenv

load_dotenv()

SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
HTTP_PATH = os.getenv("HTTP_PATH")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")


def execute_query(query: str, params: dict | None = None) -> list[dict]:
    if not all([SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN]):
        raise ValueError(
            "Variaveis de ambiente SERVER_HOSTNAME, HTTP_PATH e "
            "ACCESS_TOKEN sao obrigatorias"
        )

    with sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or {})
            df = cursor.fetchall_arrow().to_pandas()
            return df.to_dict(orient="records")
