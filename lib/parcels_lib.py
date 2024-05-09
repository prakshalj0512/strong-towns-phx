from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import *
import geopandas as gpd
from bs4 import BeautifulSoup
import requests


BASE_DIR = 'data/'

# # Initialize SparkSession
spark = SparkSession.builder \
    .appName("tax_data") \
    .getOrCreate()


def read_func(file_name, format='csv'):
    return spark.read.format(format).option('header', True).load(f'{BASE_DIR}{file_name}')

def write_func(df, file_name, format='csv'):
    (
        df
        .coalesce(1).write.format(format).mode('overwrite').option('header',True)
        .save(f'{BASE_DIR}{file_name}')
    )


# Create a function to map property use codes to categories
def map_category(property_use_code):
    for code_range, category in code_ranges.items():
        if code_range[0] <= int(property_use_code) <= code_range[1]:
            return category
    return None


def map_property_use_code(property_use_code):
    for code_range, title in property_use_mapping.items():
        if code_range[0] <= int(property_use_code) <= code_range[1]:
            return title
    return 'N/A'
