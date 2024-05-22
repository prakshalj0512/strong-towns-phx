from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Import necessary functions from PySpark
from pyspark.sql.types import StringType
from pyogrio import read_dataframe
from lib.parcels_lib import *
from lib.parcel_udfs import *

# Initialize SparkSession
# spark = SparkSession.builder.appName("categorize_parcel_taxes").getOrCreate()

################
# table read
################
raw_100 = (
    read_func('bronze/raw_100')
    .withColumn('legal_class', concat(col('LAND_1ST'), lit('.'), col('LAND_RATIO_1ST')))
    .select(
        col('parcel_no'),
        col('SITUS_CITY').alias('City'),
        col('legal_class')
    )
)

census_df = read_func('source/qgis_census_tracts.csv')
treasurer_verified_tax_data_2023 = read_func('bronze/treasurer_verified_tax_data_2023')
Parcels_All_PUCs = read_func('bronze/Parcels_All_PUCs')

# zip code & city validation
zips = read_func('source/uszips.csv').select(col('zip'), col('city').alias('zip_city'), 'state_id')

################
# logic to update PUCs for ALL lots & validate zip & city accuracy
################
valid_df = (
    treasurer_verified_tax_data_2023.alias('a')
    .join(raw_100.alias('raw_100'), on='parcel_no')
    .join(Parcels_All_PUCs, on='parcel_no', how='left')
    .join(
        zips.alias('b'),
        how='left',
        on=((trim(col('a.situs_zip'))==trim(col('b.zip'))) & (lower(col('raw_100.City'))==lower(col('b.zip_city'))))
    )
    .withColumn(
        'property_use_code',
        when(col('PUC').isNotNull() & (col('PUC') != col('property_use_code')),
            col('PUC')).otherwise(col('property_use_code'))
    )
    .withColumn
    (
        'is_valid_zip_city',
        when(col('state_id').isNull(), lit('N'))
        .otherwise(lit('Y'))
    )
    .filter(col('is_valid_zip_city')=='Y')
    .drop('is_valid_zip_city', 'zip', 'zip_city', 'state_id', 'PUC')
)

# Register the UDF; # Apply the UDF to create the new column
map_property_use_udf = udf(map_property_use_code, StringType())
valid_df = valid_df.withColumn('minor_category', map_property_use_udf(col('property_use_code')))

# Register the UDF; # Apply the mapping function to create the major_category column
map_category_udf = udf(map_category, StringType())
valid_df = valid_df.withColumn('major_category', map_category_udf(col('property_use_code')))

# remove non-productive categories (govt, agri)
productive_parcels_df = (
    valid_df
    .filter(~col('major_category').isin(['Government', 'Agricultural']))
    .withColumn(
        'major_category',
        when(col('major_category').isin(['TBD', 'Other']), lit('Other'))
        .otherwise(col('major_category'))
    )
)

# remove instances where 1 APN maps to multiple census tracts
dup_apns = (
    census_df
    .groupBy('apn')
    .count()
    .filter(col('count') > 1)
    .select('APN').distinct()
)

clean_census_df = (
    census_df
    .join(dup_apns, on='apn', how='leftanti')
    .withColumnRenamed('apn', 'parcel_no')
    .select('parcel_no', 'TRACTCE').distinct()
)

join_df = (
    productive_parcels_df
    .join(clean_census_df, on='parcel_no', how='left')
)


write_func(join_df, f'gold/census_categorized_tax_data_2023')
