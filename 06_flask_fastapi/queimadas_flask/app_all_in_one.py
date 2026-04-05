import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from databricks import sql

load_dotenv()

TABLE_NAME = "workspace.gold.queimadas_uf"
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))

app = Flask(__name__)


class QueryMaker:
    def __init__(
        self, uf: str | None = None, year: int | None = None, limit: int = 100
    ):
        self.uf = uf.upper() if uf else None
        self.year = year
        self.limit = limit

    def _generate(self) -> tuple:
        query = f"""
        SELECT sigla_uf, bioma, mes, ano, total_focos
        FROM {TABLE_NAME}
        """

        params: dict = {"limit": self.limit}
        if self.uf:
            query += " WHERE sigla_uf = :uf"
            params["uf"] = self.uf

        if self.year:
            query += " WHERE ano = :year" if not self.uf else " AND ano = :year"
            params["year"] = self.year

        query += " ORDER BY total_focos DESC LIMIT :limit"

        return query, params

    def run(self) -> list[dict]:
        query, params = self._generate()

        with sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_ACCESS_TOKEN,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                df = cursor.fetchall_arrow().to_pandas()
                return df.to_dict(orient="records")


@app.route("/queimadas", methods=["GET"])
@app.route("/queimadas/<uf>", methods=["GET"])
def get_queimadas(uf: str = None):
    try:
        year = request.args.get("year", type=int)
        limit = request.args.get("limit", DEFAULT_LIMIT, type=int)
        return jsonify(QueryMaker(uf=uf, year=year, limit=limit).run())
    except ValueError as e:
        return jsonify({"error": f"Query param deve ser inteiro. {e}"}), 422
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
