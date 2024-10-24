# ***********************************************************************************
# A Collection of Frequently Used Queries
# ***********************************************************************************

import os
import pandas as pd
import datetime as datetime

from utils.utils import timing
from utils.files import load_csv, save_csv
from utils.storage import data_directory_fs, run_pdbq

# ***********************************************************************************
# User Web Views and Purchases Tables
# ***********************************************************************************

upv_table = 'lmfr-ddp-dwh-prd.web_activity.vf_session_hit_product'

upp_table = 'lmfr-ddp-dwh-prd.web_order.vf_order_merged'
upp_table_detail = 'lmfr-ddp-dwh-prd.web_order.vf_order_merged_detail'


# ***********************************************************************************
# Loading and Savind Query Results
# ***********************************************************************************

def make_data_filename(data_name, data_date, ext='csv'):
    return f"{data_name}-{data_date}.{ext}"


def make_data_dir(bu, data_dir):
    directory = data_directory_fs(bu, target=data_dir)
    return directory


def load_data(data_name, data_dir, data_date, ext='csv'):
    name = make_data_filename(data_name, data_date, ext=ext)
    pathname = os.path.join(data_dir, name)
    df = load_csv(pathname)
    return df


def save_data(df, data_name, data_dir, data_date, ext='csv'):
    name = make_data_filename(data_name, data_date, ext=ext)
    pathname = os.path.join(data_dir, name)
    print(pathname)
    save_csv(df, pathname)

    return True


# ***********************************************************************************
# User Product Purchases
# ***********************************************************************************

# Date format: yyyy-mm-dd

def products_purchased_query(start_date, end_date):
    query = f"""
    SELECT om.num_cli as clientCode, om.id_cmde_web as webOrderCode,
    omd.num_art as productCode, mnt_total as productCost,
    cod_status_proj as orderStatus, num_vendeur as vendorCode,
    nom_vendeur as vendorName, nbr_qte as quantity, is_3P as status3P,
    omd.dat_cre as orderDate, omd.dat_expedition as shippingDate
    FROM `lmfr-ddp-dwh-prd.web_order.vf_order_merged` as om
    INNER JOIN `lmfr-ddp-dwh-prd.web_order.vf_order_detail_merged`  as omd
    ON om.id_cmde_web = omd.id_cmde_web
    WHERE dat_cre >= '{start_date}'
    AND dat_cre <= '{end_date}'
    """

    return query

# ----------------------------------------------------------------------------------

@timing
def query_products_purchased(bu, start_date, end_date):
    'Returns the web products purchased for the specified BU and date rrange.'

    query = products_purchased_query(start_date, end_date)
    df = run_pdbq(query)

    return df


# ----------------------------------------------------------------------------------

def load_products_purchased(bu, data_dir, data_date, ext='csv'):
    dirname = os.path.join(bu, 'purchases', data_date[:4])
    df = load_data('products-purchased', data_dir, data_date, ext='csv')
    return df


# ----------------------------------------------------------------------------------

def save_products_purchased(df, bu, data_date, ext='csv'):
    'Saves df to the purchases directory of bu for that year.'

    dirname = os.path.join('purchases', data_date[:4])
    data_dir = make_data_dir(bu, dirname)

    print(f'Saving product purchases for {data_date} to:')
    save_data(df, 'products-purchased', data_dir, data_date, ext='csv')

    return True


def month_date_range(year, month):
    'Return start and end dates of a particular month.'

    ts = pd.date_range('{}-{}'.format(year, month), periods=1, freq='M')[0]
    end_date = ts.strftime("%Y-%m-%d")
    month = f'0{month}' if month < 10 else str(month)
    start_date = f'{year}-{month}-01'
    return (start_date, end_date)


def monthly_date_ranges(year, n=12):
    'Return start and end dates of each month in a year.'

    return [month_date_range(year, month+1) for month in range(n)]


def retrieve_products_purchased(bu='lmfr'):
    'Queries and saves purchased products from 2020-2019.'

    for year in ['2020', '2021', '2022', '2023']:
        print (f'\nProcessing purchases for {year}:')
        month_range = monthly_date_ranges(year)
        for month_entry in month_range:
            start_date = month_entry[0]
            end_date = month_entry[1]
            print (f'\nRetrieving purchases for {start_date}')
            df = query_products_purchased(bu, start_date, end_date)
            save_products_purchased(df, bu, start_date)

    return True


# **************************************************************************
# End of File
# **************************************************************************
