################################
# Convert shp file from assessor into parquet with APN & PUC col for getting latest PUCs
# https://data-maricopa.opendata.arcgis.com/datasets/c937f17330f64e64abd41976fc8bb17f/about
################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyogrio import read_dataframe

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.parcels_lib import *
from lib.parcel_udfs import *


shapefile_path='/Users/prakshaljain/Desktop/strong_towns/data/strong-towns-phx/data/source/Parcels/Parcels_All.shp'
assessor_parcels_shp = read_dataframe(shapefile_path)

col_list = ['APN', 'PUC', 'City', 'ZipCode']

Parcels_All_PUCs = (
    spark
    .createDataFrame(assessor_parcels_shp[col_list])
    .withColumnRenamed('APN', 'parcel_no')
)

write_func(Parcels_All_PUCs, 'bronze/Parcels_All_PUCs')
