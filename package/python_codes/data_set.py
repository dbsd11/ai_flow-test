import os

class DataSets:
    def __init__(self):
        super().__init__()
    
    def collect_data_file_dir(self):
        """
        Collect the example data file.
        """
        # Example data sets are under the following data set path.
        data_set_dir = '{}/data_set/'.format(os.environ['ENV_HOME'])
        # First output result file is under the following output path.
        output_dir = '{}/output/{}/'.format(os.environ['ENV_HOME'], os.environ['TASK_ID'])
        return data_set_dir, output_dir
