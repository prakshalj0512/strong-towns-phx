from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.parcels_lib import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName('raw_100') \
    .getOrCreate()


assessor_1 = read_func('source/st42060_100/st42060_100_1.csv')
assessor_2 = read_func('source/st42060_100/st42060_100_2.csv')
assessor_3 = read_func('source/st42060_100/st42060_100_3.csv')

valid_cols = [
    'RECORD_TYPE', 'BOOK', 'MAP', 'ITEM', 'PROPERTY_STATUS', 'PROPERTY_USE_CODE', 'TAX_AREA_CODE', 'EXEMPTION_CODE',
    'EXEMPTION_INDICATOR', 'SECTION', 'TOWNSHIP', 'RANGE', 'SECTION_QUARTER', 'LOT', 'BLOCK', 'TRACT', 'SITUS_STREET_NUMBER',
    'SITUS_STREET_DIRECTION', 'SITUS_STREET_NAME', 'SITUS_STREET_TYPE', 'SITUS_STREET_SUFFIX', 'SITUS_STREET_POSTDIR', 
    'SITUS_SUITE', 'SITUS_CITY', 'SITUS_ZIP', 'OWNERS_NAME', 'OWNERS_DEED_NUMBER', 'OWNERS_DEED_DATE', 'CONTOS_NAME', 
    'CONTOS_DEED_NUMBER', 'CONTOS_DEED_DATE', 'OWNERS_ADDRESS_1ST_LINE', 'OWNERS_ADDRESS_2ND_LINE', 'OWNERS_CITY', 'OWNERS_STATE',
    'OWNERS_ZIP_CODE', 'LPV_PERCENT', 'LPV_AMOUNT', 'LPV_ASSESSED_VALUE', 'LAND_ASSESSMENT_PERCENT', 'LAND_FULL_CASH_VALUE', 
    'LAND_ASSESSED_VALUE', 'IMP_ASSESSMENT_PERCENT', 'IMP_FULL_CASH_VALUE', 'IMP_ASSESSED_VALUE', 'WVD_VET_INDICATOR', 
    'WID_VET_LPV_EXEMPTION_ASSESSED_VALUE', 'WID_VET_FPV_EXEMPTION_ASSESSED_VALUE', 'ACRES',
    # 'Column1', 'Column2', 'Column3', 'CANCEL_OR_CREATATION_DATE', 'CURRENT_YEAR_PROPERTY_STATUS', 
    # 'LAND_SOURCE', 'LAND_OVERRIDE_CODE', 'IMPROVEMENT_SOURCE', 'IMPROVEMENT_OVERRIDE_CODE', 'LAND_1ST', 
    # 'LAND_RATIO_1ST', 'LAND_2ND', 'LAND_RATIO_2ND', 'LAND_3RD', 'LAND_RATIO_3RD', 'LAND_4TH', 'LAND_RATIO_4TH', 
    # 'IMPROVEMENT_1ST', 'IMPROVEMENT_RATIO_1ST', 'IMPROVEMENT_2ND', 'IMPROVEMENT_RATIO_2ND', 'IMPROVEMENT_3RD', 
    # 'IMPROVEMENT_RATIO_3RD', 'IMPROVEMENT_4TH', 'IMPROVEMENT_RATIO_4TH', 'LAND_AREA_TYPE', 'NEIGHBORHOOD_CODE', 
    # 'MARKET_AREA_CODE', 'MCR_NUMBER', 'SUBDIVISION_NAME', 'MAILING_DATE', 'ECONOMIC_UNIT', 'NUMBER_OF_UNITS'
]

unioned_assessor_data = (
    assessor_1.select(valid_cols)
    .union(assessor_2.select(valid_cols))
    .union(assessor_3.select(valid_cols))
)

raw_100 = unioned_assessor_data.filter(col('record_type')==100)

# Pad book to 3 digits
raw_100 = (
    raw_100
    .withColumn('situs_zip', substring('situs_zip', 0, 5))
    .withColumn('property_use_code', lpad(raw_100['property_use_code'], 4, '0'))
    .withColumn('property_use_code_major', substring('property_use_code', 0, 2))
    .withColumn('book', lpad(col('book'), 3, '0'))
    .withColumn('map', lpad(col('map'), 2, '0'))
    .withColumn(
        'item', 
        when(col('item').rlike(r'^\d{1,2}$'), lpad(col('item'), 3, '0'))
        .otherwise(col('item'))
    )
    .withColumn('parcel_no', concat(col('book'),col('map'),col('item')))
    .withColumn('acres_clean', col('acres') / 43560 / 1000)
)

# drop records where PUC is null; those records are created when parcels split
raw_100 = raw_100.filter(~col('property_use_code').isNull())

write_func(raw_100, 'bronze/raw_100')
