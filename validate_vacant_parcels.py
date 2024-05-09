from lib.parcels_lib import *
from pyogrio import read_dataframe


spark = SparkSession.builder.appName("tax_data").getOrCreate()

################
# table read
################
# df = read_func('tableau_tax_data_2023_20240403184829')
df = read_func('bronze/backfilled_tax_data_2023')


shapefile_path='/Users/prakshaljain/Desktop/strong_towns/data/strong-towns-phx/data/source/Parcels/Parcels_All.shp'
assessor_parcels_shp = read_dataframe(shapefile_path)
selected_columns = ['APN', 'PUC']  # Add columns you want to select
assessor_parcels_df = spark.createDataFrame(assessor_parcels_shp[selected_columns])

################
# retrieve updated PUCs
################

vacant_lots = df.filter(col('major_category')=='Vacant')

# join vacant lots with tableau data
ans = vacant_lots.join(assessor_parcels_df, on=vacant_lots['parcel_no']==assessor_parcels_df['apn'])
updated_lots = ans.filter(col('property_use_code')!=col('puc')).select(col('APN').alias('parcel_no'), col('PUC').alias('updated_puc'))

################
# table writes
################
write_func(updated_lots, 'silver/updated_vacant_pucs_2023')