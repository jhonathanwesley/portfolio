# Incremental Pipeline - ETL Sales and Shipping

- This project is an incremental data pipeline, created to collect, transform, and store order and shipping data from a Cartpanda API, using Apache Airflow for orchestration and Google BigQuery as a data warehouse. It was structured to run every 2 hours, keeping the database always updated and ensuring efficient data processing. I will explain each part simply and directly!

> Main advantages of the pipeline:

- Incrementality: Only inserts new data, avoiding duplication and reducing unnecessary processing.

- Scalability: Can handle large volumes of data thanks to Airflow and BigQuery.

- Fault tolerance: Has retry and timeout strategies to circumvent and avoid communication failures with the API.

- Automation: Executes programmatically without the need for manual intervention.

- Organization in stages: Divides responsibilities into different tasks, making the code cleaner and easier to maintain.

How the pipeline works:

- 1️⃣ Data collection (***load_staging_data***):

``` Makes paginated calls to the Cartpanda order API.

Builds a Pandas DataFrame with only the important columns: _ID_, _order value_, _discount_, _status_, _payment method_, etc.

Adds a control column (_process_status_) to indicate whether the data has already been processed or not.

Saves this data in the BigQuery staging table.

```

- 2️⃣ Incremental insertion (***insert_new_data***):

``` Inserts only the new records into the final table, checking if the ID already exists or not. This avoids duplicates.

```

- 3️⃣ Incremental Processing (***process_incremental_data***):

```
Checks which records have not yet been processed.
Makes new API calls to retrieve additional details from these records.
Retries up to 3 times when the API fails, waiting between attempts to avoid overload.
Saves the processed data in a temporary table of already processed data.

```

- 4️⃣ Final Update (***update_final_table***):

```
Updates the final table in BigQuery with the processed information.
Marks the records as "processed" after they are processed.

Orchestration with Airflow:

load_staging_task → Loads raw data into the staging area.
insert_data_task → Inserts only new data into the final table.
process_data_task → Performs incremental processing of records not yet processed.

``` `update_final_task` → Updates the final table with already processed data.

```

> Each of these tasks depends on the previous one, forming a linear pipeline, where each step prepares the data for the next. This ensures that the data flow is consistent and organized.

- Main Skills applied during the project:

1. Efficient data collection and processing via API.

2. Use of BigQuery as a scalable repository (Data Warehouse).

3. Building robust pipelines with Airflow.

4. Data transformations using Pandas.

5. Implementation of incremental processes for performance gains.

6. Logic and tactics for architecting data solutions.

7. Python development.
