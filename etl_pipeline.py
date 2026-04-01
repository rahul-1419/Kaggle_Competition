import gdown
import zipfile
import pandas as pd
import re
from sqlalchemy import create_engine 


def data_extractation(file_id):
    url = f"https://drive.google.com/uc?id={file_id}"
    gdown.download(url,'dataset/data.zip',quiet=False)

    with zipfile.ZipFile('dataset/data.zip', 'r') as zip_ref:
        zip_ref.extractall('dataset/raw')

    inventory = pd.read_csv(r'dataset\raw\inventory_snapshots_2024.csv')
    product = pd.read_csv(r'dataset\raw\products_2024.csv')
    promotion = pd.read_csv(r'dataset\raw\promotions_2024.csv')
    sale_q1 = pd.read_csv(r'dataset\raw\sales_2024_q1.csv')
    sale_q2 = pd.read_csv(r'dataset\raw\sales_2024_q2.csv')
    sale_q3 = pd.read_csv(r'dataset\raw\sales_2024_q3.csv')
    sale_q4 = pd.read_csv(r'dataset\raw\sales_2024_q4.csv')

    return(inventory, product, promotion, sale_q1, sale_q2, sale_q3, sale_q4)



def extract_numeric(value):
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

def common_cleaning(row):
    if row['size'] == 'Each':
        row['size'] = '1 ct'
    
    row['size'] = extract_numeric(row['size'])
    
    return row

def data_transform(inventory, product, promotion, sale_q1, sale_q2, sale_q3, sale_q4):
    print('Data Transforming Start.......')
    sales = pd.concat([sale_q1,sale_q2,sale_q3,sale_q4])
    sales['promo_type'].fillna('No Promo', inplace=True)
    sales['promo_id'].fillna('No Promo', inplace=True)
    sales = sales.apply(common_cleaning, axis=1)
    sales['sale_datetime'] = pd.to_datetime(sales['sale_datetime'], format='mixed')


    promotion['start_date'] = pd.to_datetime(promotion['start_date'])
    promotion['end_date'] = pd.to_datetime(promotion['end_date'])
    promotion['duration'] = (promotion['end_date'] - promotion['start_date']).dt.days


    inventory['snapshot_date'] = pd.to_datetime(inventory['snapshot_date'])
    

    product = product.apply(common_cleaning, axis=1)

    print('Data Transforming Sucessful')

    return(inventory, product, promotion, sales)


def data_load(inventory, promotion, sales):

    print('Data Loading.......')
    
    transactions = sales[[
    'receipt_id','store_id','sale_datetime',
    'cashier_name','tender_type','customer_segment'
    ]].drop_duplicates()

    line_items = sales[[
    'receipt_id','product_upc', 'line_number',
    'quantity','unit_price_effective',
    'line_subtotal','tax_amount'
    ]].drop_duplicates()

    stores = sales[[
    'store_id','store_name','store_address',
    'store_city','store_state','store_zip'
    ]].drop_duplicates()

    products = sales[[
    'product_upc','product_name','brand',
    'department_name','category_name','size', 'unit',
    'pack_size','regular_price','unit_cost','vendor_name'
    ]].drop_duplicates()

    vendors = sales[[
    'vendor_name','vendor_phone'
    ]].drop_duplicates()

    inventory_df = inventory[[
    'snapshot_date','store_id','product_upc',
    'on_hand_qty','inventory_cost_value'
    ]]

    promo = promotion[[
    'promo_id','product_upc','promo_type',
    'discount_percent','start_date','end_date','duration'
    ]]

    engine = create_engine(
    "postgresql://postgres:Guggli190502@localhost:5432/grocery_database"
    )

    inventory_df.to_sql('inventory_snapshots', engine, if_exists='replace', index=False)

    line_items.to_sql('line_items', engine, if_exists='replace', index=False)

    promo.to_sql('promotions', engine, if_exists='replace', index=False)

    products.to_sql('products', engine, if_exists='replace', index=False)

    transactions.to_sql('transactions', engine, if_exists='replace', index=False)

    stores.to_sql('stores', engine, if_exists='replace', index=False)

    vendors.to_sql('vendors', engine, if_exists='replace', index=False)

    print('Data Load Suceefully')
