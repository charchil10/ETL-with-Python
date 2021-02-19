"""This module contains Google BigQuery to MySQL operator."""
from typing import Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.decorators import apply_defaults
import csv, os
import pandas as pd
from datetime import datetime, timedelta, date
from pathlib import Path

class BigQueryToParquet(BaseOperator):
    """
    Fetches the data from a BigQuery table (normalize all the json columns)
    and store the file in efficient parquet format.
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :type max_results: str
    :param temp_directory_location: file path for downloaded data
    """

    template_fields = (
        'project_id',
        'dataset_id',
        'table_id',
        'location',
        'temp_directory_location',
        'impersonation_chain',
    )

    @apply_defaults
    def __init__(
        self,
        *,  # pylint: disable=too-many-arguments
        project_id: str,
        dataset_id: str,
        table_id: list,
        temp_directory_location: str,
        selected_fields: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        replace: bool = False,
        batch_size: int = 75000,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.temp_directory_location = temp_directory_location
        self.replace = replace
        self.delegate_to = delegate_to
        self.batch_size = batch_size
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.dataset_id=dataset_id
        self.table_id =table_id
        self.project_id=project_id
        

    def _bq_get_data(self, table: str):
        
        def convert_timestamp(x):
            return pd.Timestamp(x/10**6,unit='s', tz= 'America/New_York')

        def expand_json(x):
            col_val = dict()
            for i in x:
                dict_to_parse = i['value']
                for key, val in dict_to_parse.items():
                    if val is not None and key in ('double_value', 'int_value', 'string_value', 'float_value'):
                        col_val[i['key']] = val
                    elif key in ('set_timestamp_micros'):
                        col_val[i['key']+'_timestamp'] = convert_timestamp(val)
            return col_val
        
        
        self.log.info('Fetching Data from:')
        self.log.info('Dataset: %s ; Table: %s', self.dataset_id, self.table_id)
        
        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        
        bq_table = hook.get_client(project_id=self.project_id, location= self.location).get_table(self.project_id +'.'+ self.dataset_id+'.'+table)
        
        columns = [field.name for field in bq_table.schema]
        timestamp_columns = [field.name for field in bq_table.schema if field.name.endswith('_timestamp')]
        json_columns = ['event_params','user_properties']
        
        df = pd.DataFrame()
        
        i = 0
        while True:
            rows = hook.list_rows(
                dataset_id=self.dataset_id,
                table_id=table,
                max_results=self.batch_size,
                selected_fields=self.selected_fields,
                start_index=i * self.batch_size,
            )
            
            chunk_df = pd.DataFrame(data=[row.values() for row in rows], columns=columns)
            
            for timestamp_col in timestamp_columns:
                chunk_df[timestamp_col] = chunk_df[timestamp_col].apply(convert_timestamp)
            
            for json_col in json_columns:
                chunk_df[json_col] = chunk_df[json_col].apply(expand_json)
            
            dict_columns = [i for i in chunk_df.columns if isinstance(chunk_df[i][0],dict)]
            
            for dict_col in dict_columns:
                new_df = pd.json_normalize(data=chunk_df[dict_col], max_level=0)
        
                chunk_df.drop(dict_col, axis = 1, inplace=True)
    
                chunk_df = pd.concat([chunk_df,new_df], axis = 1)
            
            if len(rows)<self.batch_size:
                df = df.append(chunk_df)
                self.log.info('Total Extracted rows: %s', len(rows) + i * self.batch_size)
                return df
            
            df = df.append(chunk_df)
            self.log.info('Extracted rows: %s', len(rows) + i * self.batch_size)
            
            i += 1

    def execute(self,context):
        for table in self.table_id:
            out_name = table+".parquet"
            
            out_dir = Path(self.temp_directory_location)
            out_dir.mkdir(parents=True, exist_ok=True)
#             if not os.path.exists(out_dir, exist_ok=True):
#                 os.mkdirs(out_dir)
            
#            full_dir = os.path.join(out_dir,out_name)
            
            self._bq_get_data(table=table).to_parquet(out_dir/out_name,'pyarrow',index = False, use_deprecated_int96_timestamps=True)
        return 