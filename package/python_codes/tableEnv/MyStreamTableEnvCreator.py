from flink_ai_flow.pyflink.user_define_executor import TableEnvCreator
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

execute_path = '/Users/bdiao/opt/miniconda3/envs/python-3.7/bin/python3'

class MyStreamTableEnvCreator(TableEnvCreator):

    def create_table_env(self):
        stream_env = StreamExecutionEnvironment.get_execution_environment()
        stream_env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
        stream_env.set_parallelism(1)
        
        t_env = StreamTableEnvironment.create(
            stream_env,
            environment_settings=EnvironmentSettings.new_instance()
            .in_streaming_mode().use_blink_planner().build())
        statement_set = t_env.create_statement_set()
        # t_env.get_config().set_python_executable(execute_path)
        # t_env.get_config().get_configuration().set_boolean(
        #     "python.fn-execution.memory.managed", True)
        t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '512m')
        t_env.get_config().get_configuration().set_string("rest.port", '8081')
        return stream_env, t_env, statement_set
