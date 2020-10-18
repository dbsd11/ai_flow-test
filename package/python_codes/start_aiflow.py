import os
import sys

import ai_flow as af
from ai_flow import ExampleSupportType, ModelType, ExampleMeta, ModelMeta, PythonObjectExecutor, BaseJobConfig
from flink_ai_flow import LocalFlinkJobConfig, FlinkPythonExecutor
from executors.ReadExampleExecutor import ReadTrainCsvExample, ReadLabelCsvExample, ReadTestCsvExample
from executors.WriteExampleExecutor import WriteTrainCsvExample, WritePredictTestExample
from executors.TrainModelExecutor import TrainModel
from executors.RunModelExecutor import PredictTestLabelExecutor
from data_set import DataSets
from tableEnv.MyStreamTableEnvCreator import MyStreamTableEnvCreator


def get_project_path():
    """
    Get the current project path.
    """
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def prepare_workflow():
    data_set_dir, output_dir = DataSets().collect_data_file_dir()
    """
    Prepare workflow: Example & Model Metadata registration.
    """
    train_example_meta: ExampleMeta = af.register_example(name='train_data',
                                                          support_type=ExampleSupportType.EXAMPLE_BATCH,
                                                          data_type='pandas',
                                                          data_format='csv',
                                                          batch_uri=data_set_dir+'/train_data.csv')
    label_example_meta: ExampleMeta = af.register_example(name='label_data',
                                                          support_type=ExampleSupportType.EXAMPLE_BATCH,
                                                          data_type='pandas',
                                                          data_format='csv',
                                                          batch_uri=data_set_dir+'/label_data.csv')
    test_example_meta: ExampleMeta = af.register_example(name='test_data',
                                                         support_type=ExampleSupportType.EXAMPLE_BATCH,
                                                         data_type='pandas',
                                                         data_format='csv',
                                                         batch_uri=data_set_dir+'/test_data.csv')
    test_output_example_meta: ExampleMeta = af.register_example(name='test_output_data',
                                                                support_type=ExampleSupportType.EXAMPLE_STREAM,
                                                                data_type='kafka',
                                                                data_format='csv',
                                                                stream_uri='localhost:9092')
    train_model_meta: ModelMeta = af.register_model(model_name='label_model',
                                                    model_type=ModelType.SAVED_MODEL)
    return train_example_meta, label_example_meta, test_example_meta, test_output_example_meta, train_model_meta


def run_workflow():
    """
    Run the user-defined workflow definition.
    """
    train_example_meta, label_example_meta, test_example_meta, test_output_example_meta, train_model_meta = prepare_workflow()

    python_job_config_0 = BaseJobConfig(
        job_name='read_train', platform='local', engine='python')

    python_job_config_1 = BaseJobConfig(
        job_name='train', platform='local', engine='python')

    flink_job_config_2 = LocalFlinkJobConfig()
    flink_job_config_2.job_name = 'test'
    flink_job_config_2.local_mode = 'python'
    flink_job_config_2.flink_home = os.environ['FLINK_HOME']
    flink_job_config_2.set_table_env_create_func(MyStreamTableEnvCreator())

    with af.config(python_job_config_0):
        python_job_0_read_train_data = af.read_example(example_info=train_example_meta,
                                                       executor=PythonObjectExecutor(python_object=ReadTrainCsvExample()))

        python_job_0_read_label_data = af.read_example(example_info=label_example_meta,
                                                       executor=PythonObjectExecutor(python_object=ReadLabelCsvExample()))

        write_train_data_example = af.register_example(name='write_train_data',
                                                       support_type=ExampleSupportType.EXAMPLE_BATCH,
                                                       data_type='pandas',
                                                       data_format='csv',
                                                       batch_uri='/tmp/write_train_data.csv')

        python_job_0_write_train_result = af.write_example(input_data=python_job_0_read_train_data,
                                                           example_info=write_train_data_example,
                                                           executor=PythonObjectExecutor(python_object=WriteTrainCsvExample()))

    with af.config(python_job_config_1):
        python_job_1_train_model = af.train(name='trainer_0',
                                            input_data_list=[
                                                python_job_0_read_train_data, python_job_0_read_label_data],
                                            executor=PythonObjectExecutor(
                                                python_object=TrainModel()),
                                            model_info=train_model_meta)

    with af.config(flink_job_config_2):
        flink_job_2_read_test_data = af.read_example(
            example_info=test_example_meta, executor=FlinkPythonExecutor(python_object=ReadTestCsvExample()))

        flink_job_2_predict_test_data = af.transform(input_data_list=[flink_job_2_read_test_data], executor=FlinkPythonExecutor(
            python_object=PredictTestLabelExecutor()))

        write_result = af.write_example(input_data=flink_job_2_predict_test_data,
                                                         example_info=test_output_example_meta,
                                                         executor=FlinkPythonExecutor(python_object=WritePredictTestExample()))

    af.stop_before_control_dependency(
        python_job_1_train_model, python_job_0_write_train_result)
    af.stop_before_control_dependency(
        write_result, python_job_1_train_model)
    workflow_id = af.run(get_project_path()+'/')
    res = af.wait_workflow_execution_finished(workflow_id)
    sys.exit(res)


if __name__ == '__main__':
    af.set_project_config_file(get_project_path() + '/project.yaml')
    run_workflow()
