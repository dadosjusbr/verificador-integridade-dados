import requests
import pandas as pd
import zipfile
import postgres as pg
from tqdm import tqdm
import os
import csv
import shutil


conn = pg.get_connection()
categorias_por_orgao = pg.consultar_db(
    conn,
    """select orgao, mes, ano, SUM(CASE WHEN categoria = 'direitos-pessoais' THEN valor ELSE 0 END) as direitos_pessoais, 
    SUM(CASE WHEN categoria = 'direitos-eventuais' THEN valor ELSE 0 END) as direitos_eventuais,
    SUM(CASE WHEN categoria = 'indenizações' THEN valor ELSE 0 END) as indenizacoes,
    SUM(CASE WHEN tipo = 'D' THEN valor ELSE 0 END) as total_descontos from remuneracoes r 
    where orgao not like 'mp%' group by orgao, mes, ano""",
)

# Abre o arquivo para escrita
caminho_arquivo = "compara-banco-cnj-erros.csv"

with open(caminho_arquivo, mode="w", newline="", encoding="utf-8") as arquivo:
    escritor_csv = csv.writer(arquivo)

    # Criando o cabeçalho
    escritor_csv.writerows(
        [
            [
                "orgao",
                "mes",
                "ano",
                "direitos_pessoais_expected",
                "direitos_pessoais_atual",
                "direitos_eventuais_expected",
                "direitos_eventuais_atual",
                "indenizacoes_expected",
                "indenizacoes_atual",
                "total_descontos_expected",
                "total_descontos_atual",
            ]
        ]
    )

for (
    orgao,
    mes,
    ano,
    direitos_pessoais_atual,
    direitos_eventuais_atual,
    indenizacoes_atual,
    total_descontos_atual,
) in tqdm(categorias_por_orgao):
    # Baixamos a planilha de contracheques a partir do backup já existente
    backup = requests.get(
        f"https://dadosjusbr-public.s3.amazonaws.com/{orgao}/backups/{orgao}-{ano}-{mes}.zip"
    )

    if not os.path.exists("./output"):
        os.mkdir("./output")

    with open(f"output/{orgao}-{ano}-{mes}.zip", "wb") as zip:
        zip.write(backup.content)

    with zipfile.ZipFile(f"output/{orgao}-{ano}-{mes}.zip", "r") as zip_ref:
        zip_ref.extractall("./output")

    # Realiza o filtro na planilha para garantir dados apenas de determinado órgão/mês/ano
    data = pd.read_excel(f"output/contracheque-{orgao}-{ano}-{mes:02}.xlsx")
    data = data[
        (data["Tribunal"] == f"{orgao}".upper())
        & (
            (data["Mês/Ano Ref."] == f"{mes}/{ano}")
            | (data["Mês/Ano Ref."] == f"{mes:02}/{ano}")
        )
    ]

    # Somamos os valores das rubricas consolidadas na planilha de contracheque
    direitos_pessoais_expected = data["Direitos Pessoais (1)"].sum()
    indenizacoes_expected = data["Indenizações (2)"].sum()
    direitos_eventuais_expected = data["Direitos Eventuais (3)"].sum()
    total_descontos_expected = data["Total de Descontos (9)"].sum()

    # Comparamos os valores por categoria da base com as rubricas genéricas/consolidadas do CNJ
    # Considerando 0.01 de tolerância devido a formatação numérica. ex.: 8133.2400000000008 e 8133.24
    if (
        abs(float(direitos_pessoais_atual) - direitos_pessoais_expected) > 0.01
        or abs(float(direitos_eventuais_atual) - direitos_eventuais_expected) > 0.01
        or abs(float(indenizacoes_atual) - indenizacoes_expected) > 0.01
        or abs(float(total_descontos_atual) - total_descontos_expected) > 0.01
    ):
        # Caso haja uma diferença maior que 0.01, listamos no csv
        with open(caminho_arquivo, mode="a", newline="", encoding="utf-8") as arquivo:
            escritor_csv = csv.writer(arquivo)

            # Escreve cada linha de dados no arquivo CSV
            escritor_csv.writerows(
                [
                    [
                        orgao,
                        mes,
                        ano,
                        direitos_pessoais_expected,
                        direitos_pessoais_atual,
                        direitos_eventuais_expected,
                        direitos_eventuais_atual,
                        indenizacoes_expected,
                        indenizacoes_atual,
                        total_descontos_expected,
                        total_descontos_atual,
                    ]
                ]
            )

    # Remove diretório
    shutil.rmtree("./output")
