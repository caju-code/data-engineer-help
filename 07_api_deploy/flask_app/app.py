import os

from flask import Flask, jsonify, request

from db import execute_query

app = Flask(__name__)

TABLE_NAME = "workspace.gold.queimadas_uf"
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/queimadas", methods=["GET"])
@app.route("/queimadas/<uf>", methods=["GET"])
def get_queimadas(uf: str = None):
    try:
        year = request.args.get("ano", type=int)
        limit = request.args.get("limit", DEFAULT_LIMIT, type=int)

        query = f"""
            SELECT sigla_uf, bioma, mes, ano, total_focos
            FROM {TABLE_NAME}
        """
        params = {"limit": limit}

        if uf:
            query += " WHERE sigla_uf = :uf"
            params["uf"] = uf.upper()

        if year:
            query += " AND ano = :year" if uf else " WHERE ano = :year"
            params["year"] = year

        query += " ORDER BY total_focos DESC LIMIT :limit"
        print(f"Query: {query}")
        print(f"Params: {params}")

        data = execute_query(query, params)
        return jsonify(data)

    except ValueError as e:
        return jsonify({"error": f"Parametro invalido. {e}"}), 422
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
