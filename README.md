1.安装依赖
    pip3 install package/whl_dependencies/ai_flow-0.1-py3-none-any.whl
    pip3 install apache-flink==1.11.1
2.设置PYTHONPATH：export PYTHONPATH=[python_codes目录路径]
3.配置环境变量：
必填：
ENV_HOME=[ai_flow目录路径]
TASK_ID=[任意整数]
可选：
REST_HOST=[Flink Rest Host，默认为localhost]
REST_PORT=[Flink Rest Port，默认为8081]
4.环境变量生效source init_env
5.启动AIFlow Master：python [ai_flow_master.py绝对路径]；