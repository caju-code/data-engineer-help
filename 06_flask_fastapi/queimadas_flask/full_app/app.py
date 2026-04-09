import os
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request

from query_maker import QueryMaker

load_dotenv()
API_PORT = os.getenv("API_PORT")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", 100))

app = Flask(__name__)


@app.route("/queimadas", methods=["GET"])
@app.route("/queimadas/<uf>", methods=["GET"])
def get_queimadas(uf: str = None) -> Response:
    try:
        year = request.args.get("year", type=int)
        limit = request.args.get("limit", DEFAULT_LIMIT, type=int)
        return jsonify(QueryMaker(uf=uf, year=year, limit=limit).run())
    except ValueError as e:
        return jsonify({"error": f"Query param deve ser inteiro. {e}"}), 422
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=API_PORT)
