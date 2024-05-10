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
    return spark.read.format(format).option('header', True).load(f'{BASE_DIR}{file_name}')

def write_func(df, file_name, format='csv'):
    (
        df
        .coalesce(1).write.format(format).mode('overwrite').option('header',True)
        .save(f'{BASE_DIR}{file_name}')
    )


