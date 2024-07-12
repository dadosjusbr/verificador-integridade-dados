from datetime import datetime
from checagens import *
import pandas as pd
import postgres
import json
import sys
import os


if "COURT" in os.environ:
    court = os.environ["COURT"]
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'COURT'.\n")
    sys.exit(1)

if "YEAR" in os.environ:
    year = os.environ["YEAR"]
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'YEAR'.\n")
    sys.exit(1)

if "MONTH" in os.environ:
    month = os.environ["MONTH"]
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'MONTH'.\n")
    sys.exit(1)

timestamp = datetime.now()


# Testes na planilha de contracheques
def contracheque_tests(resource, summary):
    report = validate(
        resource,
        format="csv",
        checks=[
            regex_check(),
            remuneration_check(),
            non_negative_check(),
        ],
    ).to_dict()

    alerts = []

    if not report["valid"]:
        result_dict = {}
        for error in report["tasks"][0]["errors"]:
            if error["note"] != "":
                id_error = error["note"]
                result = {
                    "id_contracheque": error["cells"][0],
                    "celula": error["cell"],
                    "linha": error["rowNumber"],
                }

            else:
                id_error = error["type"]
                result = {"descricao": error["message"]}

            if id_error in result_dict.keys():
                result_dict[id_error].append(result)
            else:
                result_dict[id_error] = [result]

        for key in result_dict.keys():
            result = {
                "quantidade_casos": len(result_dict[key]),
                "casos": result_dict[key],
            }

            alerts.append(
                f"('{court}', {month}, {year}, '{timestamp}', '{key}', '{json.dumps(result)}')"
            )

    result = expect_remuneration_to_equal_summary(resource.path, summary)
    if result is not None:
        alerts.append(
            f"('{court}', {month}, {year}, '{timestamp}', '{result[0]}', '{json.dumps(result[1])}')"
        )

    return alerts


# Testes na planilha de remunerações
def remuneracao_tests(resource, summary):
    report = validate(resource, format="csv").to_dict()
    result_dict = {}
    alerts = []

    if not report["valid"]:
        for error in report["tasks"][0]["errors"]:
            id_error = error["type"]
            result = {"descricao": error["message"]}

            if id_error in result_dict.keys():
                result_dict[id_error].append(result)
            else:
                result_dict[id_error] = [result]

        for key in result_dict.keys():
            result = {
                "quantidade_casos": len(result_dict[key]),
                "casos": result_dict[key],
            }

            alerts.append(
                f"('{court}', {month}, {year}, '{timestamp}', '{key}', '{json.dumps(result)}')"
            )

    result = entries_by_member_check(resource.path)
    if result is not None:
        alerts.append(
            f"('{court}', {month}, {year}, '{timestamp}', '{result[0]}', '{json.dumps(result[1])}')"
        )

    checks = [
        expect_R_B_to_equal_summary,
        expect_R_O_to_equal_summary,
        expect_D_to_equal_summary,
    ]

    for check in checks:
        result = check(resource.path, summary)
        if result is not None:
            alerts.append(
                f"('{court}', {month}, {year}, '{timestamp}', '{result[0]}', '{json.dumps(result[1])}')"
            )

    return alerts


def main():
    stdin = [f.rstrip() for f in sys.stdin.readlines()]
    for line in stdin:
        if '"pr"' in line:
            data = json.loads(line)
            break

    path = data["pr"]["pacote"]
    summary = data["sumario"]

    alerts = validate_csv_in_zip(path, "contracheque.csv", contracheque_tests, summary)

    alerts += validate_csv_in_zip(path, "remuneracao.csv", remuneracao_tests, summary)

    if len(alerts) != 0:
        command = (
            "INSERT INTO alertas (orgao, mes, ano, timestamp, id_alerta, detalhes) VALUES "
            + ",".join(alerts)
        )
        
        conn = postgres.get_connection()
        postgres.run_db(conn, command)


if __name__ == "__main__":
    main()
