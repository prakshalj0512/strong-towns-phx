from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import requests
import re
from datetime import datetime
from lib.parcels_lib import *

# Function to make HTTP GET request
def make_request(parcel_no):
    url = f'https://treasurer.maricopa.gov/parcel/default.aspx?Parcel={parcel_no}'
    print(url)
    try:
        response = requests.get(url)
        x=response.text
        match = re.search(r'\$\d+(,\d+)*\.\d+', x)
        dollar_amount = match.group(0)
        cleaned_string = re.sub(r'[^0-9.]', '', dollar_amount)
        return cleaned_string
    except Exception as e:
        return str(e)
        

# Initialize SparkSession
spark = SparkSession.builder \
    .appName('backfill_tax_data') \
    .getOrCreate()

################
# table read
################
consolidated_tax_data_2023 = read_func('../../Secured_Master/consolidated_tax_data_2023')

################
# parcels to recalculate
################
valid_taxes_df = consolidated_tax_data_2023.filter(col('tax_2023').rlike(r'^[0-9]+(\.[0-9]+)?$'))

need_to_recalculate = consolidated_tax_data_2023.join(valid_taxes_df, on='parcel_no', how='leftanti')

print(f'NEED TO RECALCULATE: {need_to_recalculate.count()}')
need_to_recalculate=need_to_recalculate.repartition(24)

################
# recalculate logic
################
recalculated_taxes = (
    need_to_recalculate
    .rdd.map(lambda row: (row['parcel_no'], make_request(row['parcel_no']))).toDF(['parcel_no', 'response_code'])
)

################
# merge old valid dataset with new recalculated
################

ans = consolidated_tax_data_2023.join(recalculated_taxes,on='parcel_no')

recalculated_df = (
    ans
    .select(
        'parcel_no',
        'situs_zip',
        'owners_name',
        'property_use_code_major',
        'property_use_code',
        'acres_clean',
        col('response_code').alias('tax_2023')
    )
    .withColumn('tax/acre', col('tax_2023')/col('acres_clean'))
)

backfilled_tax_data_df = valid_taxes_df.union(recalculated_df)

write_func(backfilled_tax_data_df, 'bronze/backfilled_tax_data_2023')
