from etl import logger
import pandas as pd
import re
from etl.entity.config_entity import DataTransformationConfig  



class DataTransformation:
    def __init__(self, config: DataTransformationConfig):
        self.config = config


    def common_cleaning(self, row):
        if row['size'] == 'Each':
            row['size'] = '1 ct'
        
        row['size'] = self.extract_numeric(row['size'])
        
        return row
    
    def extract_numeric(self, value):
        if isinstance(value, str):
            value = value.strip()
            
            #Fraction
            if re.search(r'\d+/\d+', value):
                frac = re.search(r'(\d+)/(\d+)', value)
                return float(frac.group(1)) / float(frac.group(2))

            num = re.search(r'\d+\.?\d*', value)
            if num:
                return float(num.group())
        
        return None

    def load_data(self):
        inventory = pd.read_csv(self.config.inventory)
        product = pd.read_csv(self.config.product)
        promotion = pd.read_csv(self.config.promotion)
        sale_q1 = pd.read_csv(self.config.sale_q1)
        sale_q2 = pd.read_csv(self.config.sale_q2)
        sale_q3 = pd.read_csv(self.config.sale_q3)
        sale_q4 = pd.read_csv(self.config.sale_q4)

        sales = pd.concat([sale_q1,sale_q2,sale_q3,sale_q4])
        logger.info('Sales is concat Successful.....')
        print(sales.shape)

        sales['promo_type'].fillna('No Promo', inplace=True)
        sales['promo_id'].fillna('No Promo', inplace=True)
        logger.info('Sales filled the null values.......')

        sales['sale_datetime'] = pd.to_datetime(sales['sale_datetime'], format='mixed')
        logger.info('Sales datetime convert sucessful.......')

        sales = sales.apply(self.common_cleaning, axis=1)
        logger.info('Sales Common cleaning done.........')



        promotion['start_date'] = pd.to_datetime(promotion['start_date'])
        logger.info('Promotion start date convert........')

        promotion['end_date'] = pd.to_datetime(promotion['end_date'])
        logger.info('Promotion end date convert.........')

        promotion['duration'] = (promotion['end_date'] - promotion['start_date']).dt.days
        logger.info('Promotion calculate duration........')


        inventory['snapshot_date'] = pd.to_datetime(inventory['snapshot_date'])
        logger.info('Inventory snapahot date convert...........')

        product = product.apply(self.common_cleaning, axis=1)
        logger.info('Product common cleaning done.........')

        sales.to_csv(self.config.sales_t, index=False)
        logger.info('Sales Dataset saved........')

        promotion.to_csv(self.config.promotion_t, index=False)
        logger.info('Promotion dataset saved.........')

        inventory.to_csv(self.config.inventory_t, index=False)
        logger.info('Inventory dataset saved...........')

        product.to_csv(self.config.products_t, index=False)
        logger.info('Product datset saved.........')

