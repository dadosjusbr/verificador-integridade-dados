# Verificador de Integridade dos Dados

Estágio e repositório de testes dos dados coletados pelo DadosJusBr utilizando [DBT](https://docs.getdbt.com/docs/introduction) e [Frictionless Framework](https://framework.frictionlessdata.io/)

## Banco de Dados

Para rodar os testes para o banco de dados:

1. Criar um ambiente virtual e instalar dependências.

```bash
sudo apt install python3.8-venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configurar conexão com o banco de dados.

O arquivo profiles.yml deve ser criado no diretório ~/.dbt/. Esse arquivo contém a configuração do perfil de conexão.

```yml
data_tests:
  target: dev
  outputs:
    dev:
      type: postgres
      host: <host>
      user: <usuario>
      password: <senha>
      port: 5432
      dbname: <nome_do_banco_de_dados>
      schema: <esquema>
      threads: 4
      keepalives_idle: 0
```

3. Rodar os seguintes comandos para testar conexão, baixar bibliotecas da ferramenta e rodar os testes :)

```bash
dbt debug
dbt deps
dbt run
AID={órgão} YEAR={ano} dbt test
```

4. Para rodar os testes para todos os órgãos e para N anos, além de [alertas percentuais](src/alertas_percentuais.py) e [comparação entre banco e CNJ](src/compara_banco_cnj.py), basta:

```bash
YEARS={lista de anos} ./banco_completo.sh
```

## Estágio

Enquanto um estágio do pipeline de coleta, a ferramenta analisa/testa o pacote de dados gerado no processo de coleta, utilizando a saída gerada pelo [Armazenador](https://github.com/dadosjusbr/armazenador).

```bash
docker build -t verificador-integridade-dados .
cat saida-armazenador.txt | docker run -i --rm -e COURT={orgao} -e YEAR={ano} -e MONTH={mes} -e POSTGRES_HOST={host} -e POSTGRES_DBNAME={dbname} -e POSTGRES_PASSWORD={password} -e POSTGRES_USER={user} -e POSTGRES_PORT={port} --name verificador-integridade-dados verificador-integridade-dados
```
