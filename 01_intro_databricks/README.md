# 01 - Intro ao Databricks

## Material da aula

O CSV utilizado na aula está na pasta `data` e o notebook utilizado é `ingestion_queimadas.ipynb`.
E [aqui](https://www.databricks.com/br/learn/training/login) você pode encontrar os cursos, certificações e treinamentos do Databricks.

## Material utilizado para obtenção dos dados

O arquivo `create_csv.py` foi utilizado previamente, para obtenção de sample dos dados de queimadas e criação do csv, em preparação para a aula.
O site que contém o dataset de queimadas completo é o [Base dos Dados](https://basedosdados.org/dataset/f06f3cdc-b539-409b-b311-1ff8878fb8d9?table=a3696dc2-4dd1-4f7e-9769-6aa16a1556b8).

### Bônus: Como criar seu próprio CSV

Caso queira criar seu próprio CSV, você pode:

1. editar a query do `create_csv.py` para buscar os dados desejados.
2. seguir [essas](https://basedosdados.org/docs/access_data_packages) instruções para configurar seu acesso `billing_id` para aos dados do Base dos Dados via BigQuery.
3. depois substitir o `billing_id` no script `create_csv.py`, instale os pacotes python e execute o código.

```bash
# crie um virtual environment e ative-o
python3 -m venv .venv
source .venv/bin/activate

# instale o pacote necessário
pip install basedosdados

# execute o script
python3 create_csv.py

# desative o venv
deactivate
```
