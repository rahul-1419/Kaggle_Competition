from etl.constants import *
from etl.utils.common import read_yaml, create_directories 
from etl.entity.config_entity import DataExtractionConfig, DataTransformationConfig, DataLoadingConfig

class ConfigurationManager:
    def __init__(
        self,
        config_filepath = CONFIG_FILE_PATH):
        
        self.config = read_yaml(config_filepath)

        create_directories([self.config.artifacts_root])


    def get_data_extraction_config(self) -> DataExtractionConfig:
        config = self.config.data_extraction

        create_directories([config.root_dir])

        data_extraction_config = DataExtractionConfig(
            root_dir=config.root_dir,
            file_id=config.file_id,
            local_data_file=config.local_data_file,
            unzip_dir=config.unzip_dir 
        )

        return data_extraction_config
    


    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation

        create_directories([config.root_dir])

        data_transformation_config = DataTransformationConfig(
            root_dir=config.root_dir,
            inventory=config.inventory,
            product=config.product,
            promotion=config.promotion,
            sale_q1=config.sale_q1,
            sale_q2=config.sale_q2,
            sale_q3=config.sale_q3,
            sale_q4=config.sale_q4,
            inventory_t=config.inventory_t,
            products_t=config.products_t,
            promotion_t=config.promotion_t,
            sales_t=config.sales_t       
            )

        return data_transformation_config
    

    def get_data_loading_config(self) -> DataLoadingConfig:
        config = self.config.data_loading

        create_directories([config.root_dir])

        data_loading_config = DataLoadingConfig(
            root_dir=config.root_dir,
            inventory = config.inventory,
            products = config.products,
            promotion = config.promotion,
            sales = config.sales,
            password = config.password
            )

        return data_loading_config