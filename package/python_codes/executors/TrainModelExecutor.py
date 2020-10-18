import os
import shutil

import numpy as np
import pandas as pd
from python_ai_flow.user_define_funcs import Executor
from ai_flow import FunctionContext, List, ExampleMeta, register_model_version, ModelMeta

from sklearn.semi_supervised import LabelPropagation
from sklearn.metrics import accuracy_score,recall_score,f1_score
from sklearn.externals import joblib

class TrainModel(Executor):
    def execute(self, function_context: FunctionContext, input_list: List) -> List:
        x_train = input_list[0]
        y_label = input_list[1]
        input_dim = 512

        x_train_columns = list()
        x_train_columns.append('face_id')
        for i in range(1, input_dim+1):
            x_train_columns.append('col' + str(i))
        trainDf = pd.DataFrame(x_train, columns=x_train_columns)
        labelDf = pd.DataFrame(y_label, columns=('face_id', 'label'))
    
        trainDf = pd.merge(trainDf,labelDf, on=['face_id'], how='inner', suffixes=('_x', '_y'))
        y_label = trainDf['label'].values.astype(int)
        trainDf = trainDf.drop('face_id', 1)
        x_train = trainDf.drop('label', 1).values

        label_prop_model = None
        score = 0.0
        while score < 0.95:
            print('before train ACC:', score)
            random_unlabeled_points = np.random.rand(len(y_label))
            random_unlabeled_points = random_unlabeled_points<0.3 # 0-1的随机数，小于0.7返回1，大于等于0.7返回0
            Y=y_label[random_unlabeled_points] # label转换之前的
            y_label[random_unlabeled_points]=-1 # 标签重置，将标签为1的变为-1

            label_prop_model = LabelPropagation()
            label_prop_model.fit(x_train,y_label)

            Y_pred = label_prop_model.predict(x_train)
            Y_pred = Y_pred[random_unlabeled_points]
            score = accuracy_score(Y,Y_pred)

            y_label[random_unlabeled_points]=Y

        model_path = os.path.dirname(os.path.abspath(__file__)) + '/model'
        print('Save trained model to {}'.format(model_path))
        if not os.path.exists(model_path):
            joblib.dump(label_prop_model, model_path)
        
        model_meta: ModelMeta = function_context.node_spec.output_model
        # Register model version to notify that cluster serving is ready to start loading the registered model version.
        register_model_version(model=model_meta, model_path=model_path)
        return []