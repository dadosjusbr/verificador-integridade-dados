from frictionless import Resource, Check, validate, errors
import pandas as pd
import re
import zipfile
import os


# Função para extrair e validar um arquivo CSV dentro de um ZIP
def validate_csv_in_zip(zip_path, csv_filename, funcao, sumario):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        # Extrair o arquivo CSV para um diretório temporário
        temp_dir = "output"
        zip_ref.extract(csv_filename, temp_dir)

        # Caminho do arquivo CSV extraído
        extracted_csv_path = os.path.join(temp_dir, csv_filename)
        resource = Resource(extracted_csv_path)

        alerts = funcao(resource, sumario)

        # Remover o arquivo extraído após a validação
        os.remove(extracted_csv_path)
        os.rmdir(temp_dir)

        return alerts


class regex_check(Check):
    Errors = [errors.CellError]
    regex = re.compile(r"^[a-zA-ZÀ-ÖØ-öø-ÿ'']+(?: [a-zA-ZÀ-ÖØ-öø-ÿ'']+)*$")

    def validate_row(self, row):
        if not re.search(self.regex, str(row["nome"])) or row["nome"] in [None, ""]:
            note = "regex_check"
            yield errors.CellError.from_row(row, note=note, field_name="nome")


class remuneration_check(Check):
    Errors = [errors.CellError]

    def validate_row(self, row):
        if not pd.isna(row["salario"]):
            salary = float(str(row["salario"]).replace(",", "."))
        else:
            salary = 0

        if not pd.isna(row["beneficios"]):
            benefits = float(str(row["beneficios"]).replace(",", "."))
        else:
            benefits = 0

        if not pd.isna(row["descontos"]):
            discounts = float(str(row["descontos"]).replace(",", "."))
        else:
            discounts = 0

        if not pd.isna(row["remuneracao"]):
            remuneration = float(str(row["remuneracao"]).replace(",", "."))
        else:
            remuneration = 0

        if not salary + benefits - discounts == remuneration:
            note = f"remuneration_check"
            yield errors.CellError.from_row(row, note=note, field_name="remuneracao")


class non_negative_check(Check):
    Errors = [errors.CellError]

    def validate_row(self, row):
        if not pd.isna(row["remuneracao"]):
            remuneration = float(str(row["remuneracao"]).replace(",", "."))
        else:
            remuneration = 0

        if remuneration < 0:
            note = f"non_negative_check"
            yield errors.CellError.from_row(row, note=note, field_name="remuneracao")


def expect_remuneration_to_equal_summary(df, data):
    if "total" not in data["remuneracoes"]:
        data["remuneracoes"]["total"] = 0

    if abs(df.remuneracao.sum() - data["remuneracoes"]["total"]) > 0.01:
        note = f"expect_remuneration_to_equal_summary"
        result = {
            "banco_de_dados": data["remuneracoes"]["total"],
            "datapackage": df.remuneracao.sum(),
        }
        return [note, result]
    return None


def expect_R_B_to_equal_summary(df, data):
    rb_soma = df[df.tipo == "R/B"].valor.sum()

    if "total" not in data["remuneracao_base"]:
        data["remuneracao_base"]["total"] = 0

    if abs(rb_soma - data["remuneracao_base"]["total"]) > 0.01:
        note = f"expect_R_B_to_equal_summary"
        result = {
            "banco_de_dados": data["remuneracao_base"]["total"],
            "datapackage": rb_soma,
        }
        return [note, result]
    return None


def expect_R_O_to_equal_summary(df, data):
    ro_soma = df[df.tipo == "R/O"].valor.sum()

    if "total" not in data["outras_remuneracoes"]:
        data["outras_remuneracoes"]["total"] = 0

    if abs(ro_soma - data["outras_remuneracoes"]["total"]) > 0.01:
        note = f"expect_R_O_to_equal_summary"
        result = {
            "banco_de_dados": data["outras_remuneracoes"]["total"],
            "datapackage": ro_soma,
        }
        return [note, result]
    return None


def expect_D_to_equal_summary(df, data):
    d_soma = df[df.tipo == "D"].valor.sum()

    if "total" not in data["descontos"]:
        data["descontos"]["total"] = 0

    # O banco armazena os descontos com valores positivos.
    if data["descontos"]["total"] > 0:
        data["descontos"]["total"] *= -1

    if abs(d_soma - data["descontos"]["total"]) > 0.01:
        note = f"expect_D_to_equal_summary"
        result = {"banco_de_dados": data["descontos"]["total"], "datapackage": d_soma}
        return [note, result]
    return None


def entries_by_member_check(df):
    df_count = (
        df[(df.tipo != "D") & (df.valor != 0)]
        .groupby(["id_contracheque"])
        .size()
        .reset_index(name="contagem")
    )

    if not df_count[df_count["contagem"] > 13].empty:
        note = f"entries_by_member_check"

        # Criar a lista de casos
        casos = df_count.apply(
            lambda row: {
                "id_contracheque": int(row["id_contracheque"]),
                "lancamentos": int(row["contagem"]),
            },
            axis=1,
        ).tolist()

        # Criar o dicionário final
        result = {"quantidade_casos": len(df_count), "casos": casos}
        return [note, result]

    return None
