from pathlib import Path

import basedosdados as bd


billing_id = "cursodataeng"

query = """
  SELECT
    dados.ano as ano,
    dados.mes as mes,
    dados.data_hora as data_hora,
    dados.bioma as bioma,
    dados.sigla_uf as sigla_uf,
    dados.id_municipio as id_municipio,
    dados.latitude as latitude,
    dados.longitude as longitude,
    dados.satelite as satelite,
    dados.dias_sem_chuva as dias_sem_chuva,
    dados.precipitacao as precipitacao,
    dados.risco_fogo as risco_fogo,
    dados.potencia_radiativa_fogo as potencia_radiativa_fogo
FROM `basedosdados.br_inpe_queimadas.microdados` AS dados
where dados.precipitacao is not null
limit 10000
"""
path = Path(__file__).parent.absolute()
sink_path = f"{path}/data/queimadas.csv"
try:
    print("iniciando o download")
    df = bd.read_sql(query=query, billing_project_id=billing_id)
    df.info()
    print(df.head())
    df.to_csv(sink_path)
    print(f"download finalizado e salvo em {sink_path}")
except Exception as e:
    raise (e)
