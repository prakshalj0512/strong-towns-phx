from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import *
import geopandas as gpd
from Secured_Master.lib.parcels_lib import *


df = read_func('consolidated_tax_data_2023_20240329135343')

# (
#     df
#     .groupBy('major_category').agg(sum('acres_clean'), sum('tax_2023'))
# ).withColumn('x', col('sum(tax_2023)').cast(DecimalType(18, 2))).show()


df = (
    df
    .filter(~col('major_category').isin(['Government', 'Agricultural']))
    .withColumn(
        'major_category',
        when(col('major_category').isin(['TBD', 'Other']), lit('Other'))
        .otherwise(col('major_category'))
    )
    .filter(col('is_valid_zip_city')=='Y')
    .drop(col('is_valid_zip_city'))
)

current_time = datetime.now().strftime("%Y%m%d%H%M%S")
write_func(df, f'tableau_tax_data_2023_{current_time}')