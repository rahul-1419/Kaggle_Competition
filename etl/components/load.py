from etl import logger
import pandas as pd
from sqlalchemy import create_engine, text
from etl.entity.config_entity import DataLoadingConfig  



class DataLoading:
    def __init__(self, config: DataLoadingConfig):
        self.config = config

    def data_loading(self):
        inventory = pd.read_csv(self.config.inventory)
        promotion = pd.read_csv(self.config.promotion)
        sales = pd.read_csv(self.config.sales)


        transactions = sales[['receipt_id','store_id','sale_datetime','cashier_name','tender_type','customer_segment']].drop_duplicates()
        line_items = sales[['receipt_id','product_upc', 'line_number','quantity','unit_price_effective','line_subtotal','tax_amount']]
        stores = sales[['store_id','store_name','store_address','store_city','store_state','store_zip']].drop_duplicates()
        products = sales[['product_upc','product_name','brand','department_name','category_name','size', 'unit','pack_size','regular_price','unit_cost','vendor_name']].drop_duplicates()
        vendors = sales[['vendor_name','vendor_phone']].drop_duplicates()
        inventory_df = inventory[['snapshot_date','store_id','product_upc','on_hand_qty','inventory_cost_value']]
        promo = promotion[['promo_id','product_upc','promo_type','discount_percent','start_date','end_date','duration']]


        engine = create_engine(f"postgresql://postgres:{self.config.password}@localhost:5432/etl_DB_2")
        logger.info('Connected to PostgreSQL DB')

        with engine.begin() as conn:

            vendors = vendors.drop_duplicates(subset=['vendor_name'])
            vendors.to_sql('vendors', conn, if_exists='append', index=False, method='multi')
            logger.info('Vendors pushed')

            vendor_df = pd.read_sql("SELECT vendor_id, vendor_name FROM vendors", conn)

            vendor_map = dict(zip(vendor_df['vendor_name'], vendor_df['vendor_id']))

            products['vendor_id'] = products['vendor_name'].map(vendor_map)

            products = products.drop(columns=['vendor_name'])

            stores = stores.drop_duplicates(subset=['store_id'])
            stores.to_sql('stores', conn, if_exists='append', index=False, method='multi')
            logger.info('Stores pushed')

            products = products.drop_duplicates(subset=['product_upc'])
            products.to_sql('products', conn, if_exists='append', index=False, method='multi')
            logger.info('Products pushed')

            transactions = transactions.drop_duplicates(subset=['receipt_id'])
            transactions.to_sql('transactions', conn, if_exists='append', index=False, method='multi')
            logger.info('Transactions pushed')

            promo.to_sql('promotions', conn, if_exists='append', index=False, method='multi')
            logger.info('Promotions pushed')

            line_items.to_sql('line_items', conn, if_exists='append', index=False, method='multi')
            logger.info('Line items pushed')

            inventory_df.to_sql('inventory_snapshots', conn, if_exists='append', index=False, method='multi')
            logger.info('Inventory pushed')