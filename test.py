#import requests
#import pandas as pd

#r = requests.get(
    #'https://www.adzuna.de/land/ad/3124936351?se=Nn2RubLT7BGX1SCNGu5MPQ&utm_medium=api&utm_source=e27c0930&v=DC540D35A6953DC2E9A8EBE56B0DF27FD0BFFBD4')
#print(r.url)
#print(r.history)


#r = requests.get('https://www.adzuna.de/land/ad/3106044734?se=Nn2RubLT7BGX1SCNGu5MPQ&utm_medium=api&utm_source=e27c0930&v=F294CA597A6141BB2E5191FDD715BB7C5AB6877A')
#redirected_url = r.text.split('link rel="preconnect" href=')[1].split('/>')[0]
#print(redirected_url)
# http://www.afaqs.com/interviews/index.html?id=572_The-target-is-to-get-advertisers-to-switch-from-print-to-TV-Ravish-Kumar-Viacom18

#r = requests.get(redirected_url)
# Start scraping from this link...

from bs4 import BeautifulSoup
import csv
import requests
import re
#import validators
import pandas as pd

# read file
from requests import Response

# with open("data.csv") as data_file:
#     reader = csv.DictReader(data_file)
#
#     for row in reader:
#         print(row)
        # if condition to check is the site redirects
        #row = row.text.split('{')[1].split('}')[0]
        #r = requests.get(row, allow_redirects=False)
        #try:
        #    r = r.status_code == 200
        #    try:
        #        request = requests.get(row['redirect_url']).text  # send http request to website (get html doc)
                # #get the text between "Auf diesen Job bewerben" and "Auf diesen Job bewerben"
        #        try:
        #            text = re.findall('(?<=Auf diesen Job bewerben)(.*?)(?=Auf diesen Job bewerben)',
        #                          request.replace('\n', '')), [0]
        #           reader["job_description"] = "available"
                    # clean text from html tags
                    #job_description = BeautifulSoup(text, "html.parser").get_text()
                    # add column with the text
                    #reader["job_description"] = job_description
        #        except ValueError:
                    #job_description = "not available"
        #            reader["job_description"] = "not available"
        #    except ValueError:
        #        reader["job_description"] = "not available"
        #except ValueError:
        #    reader["job_description"] = "not available"
#data_file.close()

import json
import findspark
findspark.init()
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession,SQLContext
#import calendar
#import time
# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
# sc = pyspark.SparkContext('local[2]')


# sc = pyspark.SparkContext()
# sqlCtxt = SQLContext(sc)
# params = dict(
#         app_key='db9ca784da33298ade0ae4bb73ea9be8',
#         app_id= 'e27c0930',
#         what = 'data',
#         salary_min=1,
#         #where = city,
#         full_time = 1,
#         #max_days_old = max_days_old,
#         results_per_page = 100
#     )
# url = 'https://api.adzuna.com/v1/api/jobs/us/search/' + str(1)
# print(url)
# resp = requests.get(url=url, params=params)
# print(resp.text)
# data = json.loads(resp.text)
# print(data)
# print(resp.status_code)
# json_rdd = sc.parallelize([resp.text])
# df_spark = sqlCtxt.read.schema(json_rdd)
#
# print(df_spark.show())

from hdfs3 import HDFileSystem
hdfs = HDFileSystem(host='http://seneca1.lehre.hwr-berlin.de', port=9870)
hdfs.ls('/user/ngoc_nguyen')

def saveHDFS(sparkDF):

    hdfs = HDFileSystem(host='http://seneca1.lehre.hwr-berlin.de', port=9870)
    hdfs.ls('/user/ngoc_nguyen')
    with hdfs.open('/career_app/jobs.csv', 'wb') as f:
        f.write(b'Hello, world!')
