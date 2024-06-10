#!/bin/bash

AID_LIST="${AID_LIST:=$(cat ./orgaos.txt)}"
YEARS="${YEARS}"

for AID in ${AID_LIST[*]}
    do
        for YEAR in ${YEARS[*]}
        do
            AID=${AID} YEAR=${YEAR} dbt test
            mv logs/dbt.log logs/${AID}-${YEAR}.log
            mv target/run_results.json logs/${AID}-${YEAR}.json
        done
    done

YEARS=${YEARS} python3 src/alertas_percentuais.py

YEARS=${YEARS} python3 src/compara_banco_cnj.py