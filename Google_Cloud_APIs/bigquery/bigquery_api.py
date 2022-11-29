from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class BigQueryUtil:

    # Construct a BigQuery client object.
    client = bigquery.Client()

    @staticmethod
    def create_table(table_id, schema):
        """
        Creates new table in BigQuery with the provided schema and table id

        :param str table_id: table name that we want to create
        :param list schema: table schema
        :return: Tells if the table was created or not
        :rtype: bool
        """
        try:
            table_schema = []
            for x in range(len(schema)):
                field_name = schema[x]['field_name']
                field_type = schema[x]['type']
                field_mode = schema[x]['mode']
                table_schema.append(bigquery.SchemaField(field_name, field_type, mode=field_mode))

            table = bigquery.Table(table_id, schema=table_schema)
            table = BigQueryUtil.client.create_table(table)
            table_name = f"{table.project}.{table.dataset_id}.{table.table_id}"
  
            return True
        except Exception as e:
            print("Exception in BigQueryUtil.create_table - {str(e)}")
            return False

    # Function to check if the table exists
    @staticmethod
    def table_exists(table):
        """
        Checks if the provided table  exists in Bigquery or not

        :param str table: table name along with project and dataset ((project.dataset.table_name))
        :return: Whether table exists or not
        :rtype: bool
        """
        try:
            BigQueryUtil.client.get_table(table)  # Make an API request.
            return True
        except NotFound:
            print("Table {table} not found")
            return False

    @staticmethod
    def execute_query(query):
        """
        Runs provided query and returns the results

        :param str query: query that needs to run in bigquery
        :return: Query results
        :rtype: list
        :raises Exception: when BigQuery client throws exception
        """
        try:
            query_job = BigQueryUtil.client.query(query)
            results = query_job.result()
            return results
        except Exception as e:
            print("Exception in BigQueryUtil.execute_query - {str(e)}")
            raise Exception(e)

    @staticmethod
    def delete_table(table_id): # table_id = 'your-project.your_dataset.your_table'
        """
        Delete provided table from bigquery

        :param str table_id: Table name (project.dataset.table) that needs to be deleted
        :raises Exception: When BigQuery client throws exception
        """
        try:
            # If the table does not exist, delete_table raises
            # google.api_core.exceptions.NotFound unless not_found_ok is True.
            BigQueryUtil.client.delete_table(table_id, not_found_ok=True)  # Make an API request.
            print("Deleted table '{}'.".format(table_id))
        except Exception as e:
            print("Exception in BigQueryUtil.delete_table - {str(e)}")
            raise Exception(e)

    @staticmethod
    def load_bq_table(file_url, table, file_format='avro', partition_type=None, partition_field=None):
        """Loads data from GCS to Bigquery table

        :param str file_url: Url from where the data needs to loaded
        :param str table: table name where the needs to be loaded
        :param str file_format: format in which the file is in, defaults to avro
        :param str partition_type: defaults to None
        :param str partition_field: defaults to None
        :return: # records that were loaded into table, total # records present in the table after loading the new data
        :rtype: tuple
        :raises Exception: When BigQuery client throws exception"""
        try:
            src_format = bigquery.SourceFormat.AVRO
            if file_format == 'parquet':
                src_format = bigquery.SourceFormat.PARQUET
            elif file_format == 'csv':
                src_format = bigquery.SourceFormat.CSV
            elif file_format == 'orc':
                src_format = bigquery.SourceFormat.ORC
            elif file_format == 'jsonl':
                src_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

            job_config = bigquery.LoadJobConfig(source_format=src_format, autodetect=True)
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]

            if partition_type and partition_field:
                time_partitioning = bigquery.table.TimePartitioning(type_=partition_type, field=partition_field)
                job_config.time_partitioning = time_partitioning

            # Make an API request.
            load_job = BigQueryUtil.client.load_table_from_uri(
                file_url, table, job_config=job_config
            )

            # Waits for the job to complete.
            results = load_job.result()

            # fetch count
            record_count = str(results.output_rows)

            # fetch total count
            destination_table = BigQueryUtil.client.get_table(table)
            total_count = destination_table.num_rows
            return record_count, total_count
        except Exception as e:
            print("Exception in BigQueryUtil.load_bq_table - {str(e)}")
            raise Exception(e)

    @staticmethod
    def insert_json_rows(table_id, data):
        try:
            errors = BigQueryUtil.client.insert_rows_json(table_id, data)
            if errors:
                raise Exception(errors)
        except Exception as e:
           print("Exception in BigQueryUtil.insert_json_rows - {str(e)}")
            raise Exception(e)
