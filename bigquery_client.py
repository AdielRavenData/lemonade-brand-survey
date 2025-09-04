#This class is designed to facilitate interactions with Google BigQuery,
#including loading data into tables and performing UPSERT operations. 

from google.cloud import bigquery
from google.oauth2 import service_account
import logging


class BigqueryClient:
    #def __init__(self, creds_path, primary_keys, tables_of_interest):
    def __init__(self,primary_keys, tables_of_interest):
        # self.path_to_cred = creds_path
        # self.creds = service_account.Credentials.from_service_account_file(
        #     self.path_to_cred
        # )
        #self.client = bigquery.Client(credentials=self.creds)
        self.client = bigquery.Client()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.info("Created BigqueryClient successfully!")
        self.field_type = {
            "string": "STRING",
            "bytes": "BYTES",
            "int": "INTEGER",
            "float": "FLOAT",
            "bool": "BOOLEAN",
            "datetime": "DATETIME",
            "datetime.date": "DATE",
            "datetime.time": "TIME",
            "dict": "RECORD",
        }
        self.primary_keys = primary_keys
        self.tables_of_interest = tables_of_interest

    def get_as_equalities(self, lst, type_dict, sep):
        all_items_string = ""
        null_types = {
            "STRING": -9999999,
            "BYTES": 0,
            "INTEGER": -999999,
            "FLOAT": -999999,
            "BOOLEAN": True,
            "DATETIME": "19981018",
            "DATE": "1998-10-18",
            "TIME": "13:45:55",
        }
        last = lst[len(lst) - 1]
        # minus_two = lst[len(lst) - 2]
        for i in lst:
            if i != last:
                if type_dict[i] == "STRING":
                    all_items_string = f"{all_items_string} IFNULL(t.{i},CAST({null_types[type_dict[i]]} AS STRING)) = IFNULL(s.{i},CAST({null_types[type_dict[i]]} AS STRING)) {sep}"
                elif (
                    type_dict[i] == "DATETIME"
                    or type_dict[i] == "DATE"
                    or type_dict[i] == "TIME"
                ):
                    all_items_string = f"{all_items_string} IFNULL(t.{i},parse_date('%Y%m%d',cast({null_types[type_dict[i]]} as string))) = IFNULL(s.{i},parse_date('%Y%m%d',cast({null_types[type_dict[i]]} as string))) {sep}"
                else:
                    all_items_string = f"{all_items_string} IFNULL(t.{i},{null_types[type_dict[i]]}) = IFNULL(s.{i},{null_types[type_dict[i]]}) {sep}"
            else:
                if type_dict[i] == "STRING":
                    all_items_string = f"{all_items_string} IFNULL(t.{i},CAST({null_types[type_dict[i]]} AS STRING)) = IFNULL(s.{i},CAST({null_types[type_dict[i]]} AS STRING))"
                elif (
                    type_dict[i] == "DATETIME"
                    or type_dict[i] == "DATE"
                    or type_dict[i] == "TIME"
                ):
                    all_items_string = f"{all_items_string} IFNULL(t.{i},parse_date('%Y%m%d',cast({null_types[type_dict[i]]} as string)))  = IFNULL(s.{i},parse_date('%Y%m%d',cast({null_types[type_dict[i]]} as string)))"
                else:
                    all_items_string = f"{all_items_string} IFNULL(t.{i},{null_types[type_dict[i]]}) = IFNULL(s.{i},{null_types[type_dict[i]]})"

        return all_items_string

    def get_keys(self, lst, type_dict):
        return self.get_as_equalities(lst, type_dict, "AND")

    def get_updates(self, lst):
        return self.get_as_equalities(lst, ",")

    def get_vals(self, lst):
        prefix_lst = ["s." + sub for sub in lst]
        lst_as_str = ",".join(prefix_lst)
        return lst_as_str

    def get_fields(self, lst):
        return ",".join(lst)

    def map_df_to_bq_schema(self, source_data):
        """
        Maps a Pandas DataFrame to a BigQuery schema.

        Args:
            source_data (dict): A dictionary containing a Pandas DataFrame and a
            dictionary of column names and data types.

        Returns:
            tuple: A tuple containing the mapped Pandas DataFrame and a list of
            BigQuery SchemaFields.

        """
        # Create a list of BigQuery SchemaFields from the column names and data types
        # in dtypes_dict.
        schema = [
            bigquery.SchemaField(col, self.field_type[dtype])
            for col, dtype in source_data["schema"].items()
            if dtype in self.field_type
        ]
        type_dict = {
            col: self.field_type[dtype]
            for col, dtype in source_data["schema"].items()
            if dtype in self.field_type
        }
        # Return a tuple containing the mapped Pandas DataFrame and the list of
        # BigQuery SchemaFields.
        return (source_data["dataframe"], schema), type_dict

    def run_query(self, sql_query, log_msg="Query Executed."):
        """
        This function executes a SQL query and logs a message.
        """
        query_job = self.client.query(sql_query)
        query_job.result()
        self.logger.info(log_msg)
    
    def build_upsert_query(
        self, table_name, target_table_id, source_table_id, table_keys, type_dict
    ):
        if table_name in self.tables_of_interest:
            primary_keys = self.primary_keys[table_name]
            non_keys = list(set(table_keys) - set(primary_keys))

            on_clause = self.get_as_equalities(primary_keys, type_dict, "AND")
            update_clause = ", ".join([f"t.{col} = s.{col}" for col in non_keys])

            dml_statement = f"""
            MERGE {target_table_id} as t
            USING {source_table_id} as s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ROW"""
        else:
            on_clause = self.get_as_equalities(table_keys, type_dict, "AND")

            dml_statement = f"""
            MERGE {target_table_id} as t
            USING {source_table_id} as s
            ON {on_clause}
            WHEN NOT MATCHED THEN INSERT ROW"""

        return dml_statement

    def load_table(
        self, data, table_id, schema=[], detect_schema=True, to_truncate=True
    ):
        """
        Params:
                table_id = <project-id>.<dataset-name>.<table-name>
                detect_schema = False to specify a schema, True otherwise
                            *currently, all data is being uploaded with
                            autodetect = True
                to_truncate = True in case we want to overwrite the
                            exisring table False otherwise
        """
        job_configuration = bigquery.LoadJobConfig(source_format="CSV")
        job_configuration.write_disposition = (
            "WRITE_APPEND" if to_truncate else "WRITE_APPEND"
        )
        if detect_schema:
            job_configuration.autodetect = detect_schema
        else:
            job_configuration.schema = schema

        job = self.client.load_table_from_dataframe(
            data, table_id, job_config=job_configuration
        )
        job.result()

        self.logger.debug(f"{table_id} inserted to Bigquery")
        
    def read_table(self, table_id, conditions=None):
        """
        Reads data from a BigQuery table.

        Args:
            table_id (str): The ID of the BigQuery table in the format 'project.dataset.table'.
            conditions (str, optional): SQL conditions to filter the data. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the queried data.
        """
        query = f"SELECT * FROM `{table_id}`"
        if conditions:
            query += f" WHERE {conditions}"

        query_job = self.client.query(query)
        results = query_job.result()
        df = results.to_dataframe()

        return df