import postgres as pg
import pandas as pd
import os
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

where = ''
if 'YEARS' in os.environ:
    years = os.getenv('YEARS').replace(" ", ", ")
    if years != '':
        where = f"and ano IN ({years})"

conn = pg.get_connection()

dados = pg.consultar_db(
    conn,
    f"""select id_orgao, c.mes, c.ano, 
    sumario->'membros', 
    sumario->'remuneracoes'->'total' from coletas c 
    where atual = true and (procinfo is null or procinfo::text = 'null') {where}""",
)

dados = pd.DataFrame(
    dados, columns=["orgao", "mes", "ano", "membros", "remuneracao_liquida"]
).fillna(0)

resultado = pd.DataFrame(
    columns=[
        "orgao",
        "mes",
        "ano",
        "ano_comparado",
        "remuneracao_liquida",
        "remuneracao_comparada",
        "percentual_diff_remuneracao",
        "caso_remuneracao",
        "membros",
        "quant_membros_comparada",
        "percentual_diff_membros",
        "caso_membros",
    ]
)

for orgao, mes, ano, membros, remuneracao_liquida in tqdm(dados[
    ["orgao", "mes", "ano", "membros", "remuneracao_liquida"]
].to_numpy()):
    # Pegamos os dados do respectivo mês a ser analisado no ano imediatamente anterior
    dados_por_mes = dados[
        (dados.orgao == orgao) & (dados.mes == mes) & (dados.ano == ano - 1)
    ]
    percentual_media = (
        (remuneracao_liquida - dados_por_mes.remuneracao_liquida.mean()) * 100
    ) / dados_por_mes.remuneracao_liquida.mean()
    percentual_media_membros = (
        (membros - dados_por_mes.membros.mean()) * 100
    ) / dados_por_mes.membros.mean()

    alerta = pd.DataFrame(
        {
            "orgao": [orgao],
            "mes": [mes],
            "ano": [ano],
            "ano_comparado": [ano - 1],
            "remuneracao_liquida": [remuneracao_liquida],
            "remuneracao_comparada": [dados_por_mes.remuneracao_liquida.mean()],
            "percentual_diff_remuneracao": [percentual_media],
            "membros": [membros],
            "quant_membros_comparada": [dados_por_mes.membros.mean()],
            "percentual_diff_membros": [percentual_media_membros],
        }
    )

    if percentual_media > 50:
        alerta["caso_remuneracao"] = "MAIOR"

    if percentual_media < -50:
        alerta["caso_remuneracao"] = "MENOR"

    if percentual_media_membros > 50:
        alerta["caso_membros"] = "MAIOR"

    if percentual_media_membros < -50:
        alerta["caso_membros"] = "MENOR"

    if "caso_remuneracao" in alerta.keys() or "caso_membros" in alerta.keys():
        resultado = pd.concat([resultado, alerta], ignore_index=True)


# Gerando csv com resultado dos alertas
resultado = resultado.sort_values(["orgao", "ano", "mes"])
resultado.to_csv("alertas-percentuais.csv", index=False)
