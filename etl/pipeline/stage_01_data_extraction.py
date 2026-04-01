from etl.config.configuration import ConfigurationManager
from etl.components.extract import DataExtraction
from etl import logger


STAGE_NAME = "Data Extraction stage"

class DataExtractionPipeline:
    def __init__(self):
        pass

    def main(self):
        config = ConfigurationManager()
        data_extraction_config = config.get_data_extraction_config()
        data_extraction = DataExtraction(config=data_extraction_config)
        data_extraction.download_file()
        data_extraction.extract_zip_file()