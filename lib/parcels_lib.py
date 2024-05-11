from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import *
import geopandas as gpd
from bs4 import BeautifulSoup
import requests

spark = SparkSession.builder.appName("parcel_lib").getOrCreate()
BASE_DIR = 'data/'


def read_func(file_name, format='csv'):
    if format == 'csv':
        return spark.read.format(format).option('header', True).load(f'{BASE_DIR}{file_name}')
    else:
        return spark.read.format(format).load(f'{BASE_DIR}{file_name}')

def write_func(df, file_name, format='csv'):
    if format == 'parquet':
        (
            df
            .write.format(format).mode('overwrite')
            .save(f'{BASE_DIR}{file_name}')
        )
    else:
        (
            df
            .coalesce(1).write.format(format).mode('overwrite').option('header', True)
            .save(f'{BASE_DIR}{file_name}')
        )



