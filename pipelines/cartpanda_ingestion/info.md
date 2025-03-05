# Pipeline Incremental - ETL Vendas e Frete

- Esse projeto é um pipeline de dados incremental, criado para coletar, transformar e armazenar dados de pedidos e frete de uma API do Cartpanda, usando o Apache Airflow para orquestração e o Google BigQuery como data warehouse. Ele foi estruturado para rodar a cada 2 horas, mantendo a base sempre atualizada e garantindo a eficiência no processamento de dados. Vou explicar cada parte, de forma simples e direta!

> Principais vantagens do pipeline:

- Incrementalidade: Só insere dados novos, evitando duplicidade e reduzindo o processamento desnecessário.
- Escalabilidade: Pode lidar com grandes volumes de dados graças ao Airflow e ao BigQuery.
- Tolerância a falhas: Tem estratégias de retry e timeout para contornar e evitar falhas de comunicação com a API.
- Automação: Executa de forma programada sem necessidade de intervenção manual.
- Organização em etapas: Divide as responsabilidades em diferentes tarefas, tornando o código mais limpo e fácil de manter.

> Como o pipeline funciona:

- 1️⃣ Coleta de dados (***load_staging_data***):

```
Faz chamadas paginadas para a API de pedidos do Cartpanda.
Constrói um DataFrame do Pandas só com as colunas importantes: _ID_, _valor do pedido_, _desconto_, _status_, _método de pagamento_, etc.
Adiciona uma coluna de controle (_process_status_) para indicar se os dados já foram processados ou não.
Salva esses dados na tabela de staging do BigQuery.
```

- 2️⃣ Inserção incremental (***insert_new_data***):

```
Insere apenas os registros novos na tabela final, verificando se o ID já existe ou não. Assim, evita duplicidade.
```

- 3️⃣ Processamento incremental (***process_incremental_data***):

```
Verifica quais registros ainda não foram processados.
Faz novas chamadas na API para buscar detalhes adicionais desses registros.
Tenta até 3 vezes quando há falha na API, esperando entre tentativas para evitar sobrecarga.
Salva os dados processados em uma tabela temporária de dados já tratados.
```

- 4️⃣ Atualização final (***update_final_table***):

```
Atualiza a tabela final no BigQuery com as informações processadas.
Marca os registros como "processed" depois que são tratados.
Orquestração com Airflow:

load_staging_task → Carrega dados brutos para o staging.
insert_data_task → Insere apenas dados novos na tabela final.
process_data_task → Faz o processamento incremental de registros ainda não tratados.
update_final_task → Atualiza a tabela final com dados já processados.
```

> Cada uma dessas tarefas depende da anterior, formando um pipeline linear, onde cada etapa prepara os dados para a próxima. Isso garante que o fluxo de dados seja consistente e organizado.

- Principais Skills aplicadas durante o projeto:

1. Coleta e tratamento eficiente de dados via API.
2. Uso de BigQuery como repositório escalável (Data Warehouse).
3. Construção de pipelines robustos com Airflow.
4. Transformações de dados usando Pandas.
5. Implementação de processos incrementais para ganho de performance.
6. Lógica e táticas de arquitetar soluções para dados.
7. Desenvolvimento em Python.
