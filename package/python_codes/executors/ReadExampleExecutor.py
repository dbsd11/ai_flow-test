import numpy as np
import pandas as pd
from ai_flow import FunctionContext, List, ExampleMeta, register_model_version, ModelMeta
from python_ai_flow.user_define_funcs import Executor
from flink_ai_flow.pyflink.user_define_executor import SourceExecutor, FlinkFunctionContext
from pyflink.table import Table, TableEnvironment

class ReadTrainCsvExample(Executor):
    def execute(self, function_context: FunctionContext, input_list: List) -> List:
        example_meta: ExampleMeta = function_context.node_spec.example_meta
        data = pd.read_csv(example_meta.batch_uri, sep=';', header=None, usecols=[1,3])
        n = data.values.tolist()
        rows = len(n)
        xx = []
        for i in range(rows):
            x = []
            x.append(n[i][0])
            yy = n[i][1].split(' ')
            for y in yy:
                x.append(float(y))
            xx.append(np.array(x))
        xx = np.array(xx)
        return [xx]

class ReadLabelCsvExample(Executor):
    def execute(self, function_context: FunctionContext, input_list: List) -> List:
        example_meta: ExampleMeta = function_context.node_spec.example_meta
        data = pd.read_csv(example_meta.batch_uri, sep=';', header=None)
        n = data.values.tolist()
        xx = np.array(n)
        return [xx]

class ReadTestCsvExample(SourceExecutor):
    def execute(self, function_context: FlinkFunctionContext) -> Table:
        table_env: TableEnvironment = function_context.get_table_env()
        path = function_context.get_example_meta().batch_uri
        ddl = """create table test_table(
                                face_id varchar,
                                feature_data varchar
                    ) with (
                        'connector.type' = 'filesystem',
                        'format.type' = 'csv',
                        'connector.path' = '{}',
                        'format.ignore-first-line' = 'false',
                        'format.field-delimiter' = ';'
                    )""".format(path)
        table_env.execute_sql(ddl)
        return table_env.from_path('test_table')