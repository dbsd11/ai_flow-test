import os

import numpy as np
import pandas as pd
from ai_flow import FunctionContext, List, ExampleMeta, register_model_version, ModelMeta
from flink_ai_flow.pyflink.user_define_executor import Executor, SourceExecutor, FlinkFunctionContext
from pyflink.table import Table, TableEnvironment, ScalarFunction, DataTypes
from pyflink.table.udf import udf, udtf

from sklearn.semi_supervised import LabelPropagation
from sklearn.externals import joblib


class PredictTestLabelExecutor(Executor):
    def execute(self, function_context: FlinkFunctionContext, input_list: List[Table]) -> List[Table]:
        t_env = function_context.get_table_env()
        table = input_list[0]
        t_env.register_function("predict", udf(f=PredictFunction(
            None), input_types=[DataTypes.STRING()], result_type=DataTypes.STRING()))
        return [table.select('face_id, predict(feature_data) as label')]


class PredictFunction(ScalarFunction):

    def __init__(self, label_prop_model: LabelPropagation):
        self.label_prop_model = label_prop_model
        if self.label_prop_model == None:
            self.open(function_context=None)

    def open(self, function_context: FunctionContext):
        if self.label_prop_model == None:
            model_path = os.path.dirname(
                os.path.abspath(__file__)) + '/model'
            self.label_prop_model = joblib.load(model_path)
        return None

    def eval(self, vec):
        if len(vec) != 0 and not vec.isspace():
            vector = [float(v) for v in vec.split(' ')]
            Y_pred = self.label_prop_model.predict([vector])
            print('Y_pred', str(Y_pred[0]))
            return str(Y_pred[0])
        return None

    def close(self):
        self.label_prop_model = None
