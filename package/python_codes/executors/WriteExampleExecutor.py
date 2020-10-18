import numpy as np
import pandas as pd
from python_ai_flow.user_define_funcs import Executor
from ai_flow import FunctionContext, List, ExampleMeta, register_model_version, ModelMeta
from flink_ai_flow.pyflink.user_define_executor import FlinkFunctionContext, SinkExecutor
from pyflink.table import Table, TableEnvironment


class WriteTrainCsvExample(Executor):
    def execute(self, function_context: FunctionContext, input_list: List) -> List:
        example_meta: ExampleMeta = function_context.node_spec.example_meta
        train_data = input_list[0]
        output_file = example_meta.batch_uri
        pd.DataFrame(train_data[:, [0, 1]], columns=('face_id', 'label')).to_csv(
            output_file, sep=";", header=False, index=False)


class WritePredictTestExample(SinkExecutor):
    def execute(self, function_context: FlinkFunctionContext, input_table: Table) -> None:
        example_meta: ExampleMeta = function_context.node_spec.example_meta
        table_env: TableEnvironment = function_context.get_table_env()
        statement_set = function_context.get_statement_set()
        table_env.execute_sql("""
               create table write_predict_test_table (
                    face_id varchar,
                    label varchar
                ) with (
                    'connector' = 'kafka',
                    'topic' = 'tianchi_write_example',
                    'properties.bootstrap.servers' = '{}',
                    'properties.group.id' = 'write_example',
                    'properties.request.timeout.ms' = '30000',
                    'format' = 'csv',
                    'scan.startup.mode' = 'earliest-offset',
                    'csv.disable-quote-character' = 'true'
                )
                """.format(example_meta.stream_uri))
        input_table.insert_into('write_predict_test_table')
        # table_env.execute_sql("""
        #        create table write_predict_test_table (
        #             face_id varchar,
        #             label varchar
        #         ) with (
        #             'connector' = 'blackhole'
        #         )
        #         """)
        statement_set.add_insert('write_predict_test_table', input_table)

