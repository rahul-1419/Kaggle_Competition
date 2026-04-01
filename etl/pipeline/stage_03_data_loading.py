from etl.config.configuration import ConfigurationManager
from etl.components.load import DataLoading
from etl import logger


STAGE_NAME = "Data Loading stage"

class DataLoadingPipeline:
    def __init__(self):
        pass

    def main(self):
        config = ConfigurationManager()
        data_loading_config = config.get_data_loading_config()
        data_loading = DataLoading(config=data_loading_config)
        data_loading.data_loading()