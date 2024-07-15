import json
import os
import csv

# Caminho do diretório
diretorio = 'logs'

# Listar todos os arquivos com extensão .json
arquivos_json = [arquivo for arquivo in os.listdir(diretorio) if arquivo.endswith('.json')]

# Abre o arquivo para escrita
caminho_arquivo = "resumo_testes_dbt.csv"

with open(caminho_arquivo, mode="w", newline="", encoding="utf-8") as csv_resumo:
    escritor_csv = csv.writer(csv_resumo)

    # Criando o cabeçalho
    escritor_csv.writerows(
        [
            [
                "orgao",
                "ano",
                "erro",
                "quantidade_erros"
            ]
        ]
    )

for arq in arquivos_json:
    # Abrir e ler o arquivo JSON
    with open(f'logs/{arq}', 'r') as file:
        data = json.load(file)

    for res in data['results']:
        if res['status'] == 'fail':
            # Remover a extensão do arquivo
            orgao_ano = arq.replace('.json', '')

            # Dividir a string pelo delimitador "-"
            orgao_ano = orgao_ano.split('-')
            
            with open(caminho_arquivo, mode="a", newline="", encoding="utf-8") as csv_resumo:
                escritor_csv = csv.writer(csv_resumo)

                # Escreve cada linha de dados no arquivo CSV
                escritor_csv.writerows(
                    [
                        [
                            orgao_ano[0],
                            orgao_ano[1],
                            res['unique_id'],
                            res['failures']
                        ]
                    ]
                )