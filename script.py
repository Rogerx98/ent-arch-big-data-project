from csv import writer
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import json
import findspark
findspark.init()
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
import pyspark
from pyspark.context import SparkContext
#import calendar
#import time
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import regexp_replace
from hdfs3 import HDFileSystem
#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("careerApp") \
    .getOrCreate()

def getPayload(url,app_id,app_key='', search_key='data',page=1,results_per_page=1000,salary_min=1,full_time=1):

    params = dict(
        app_key=app_key,
        app_id= app_id,
        what = search_key,
        salary_min=salary_min,
        #where = city,
        full_time = full_time,
        #max_days_old = max_days_old,
        results_per_page = results_per_page
    )
    url = url + str(page)
    resp = requests.get(url=url, params=params)

    if resp.status_code==200:
        data = json.loads(resp.text)
        print(data)
        print(resp.status_code)
        result = pd.json_normalize(data, record_path=["results"])
    else:
        print('Error status code'+ ' '+str(resp.status_code))
    return result

#
def load_data(data):
    properties = ['title', 'contract_type', 'description','company.display_name' ,
                  'salary_max', 'salary_min', 'location.display_name', 'salary_is_predicted', 'redirect_url','created','category.tag']

    df = pd.DataFrame(columns=[p for p in properties])
    for p in properties:
        if p in (data.columns.values):
            continue
        else:
            data[p] = np.nan
    data = data[[p for p in properties]]
    df.append(data, ignore_index=True)
    return data

def getFullDesc(df):
    dfc = df.copy()
    dfc["full_description"] = ""
    for index, row in dfc.iterrows():
        r = requests.get(row['redirect_url'], allow_redirects=False)

        if r.status_code == 200 or r.status_code == 303:
            request = requests.get(row['redirect_url']).text  # send http request to website (get html doc)

            if bool(re.search('<section class="adp-body mx-4 mb-4 text-sm md:mx-0 md:text-base md:mb-0">', request))==True:

                desc = request.split('<section class="adp-body mx-4 mb-4 text-sm md:mx-0 md:text-base md:mb-0">')[1].split('</section>')[0]
                dfc.loc[index,'full_description'] = desc
            elif bool(re.search('<section class="adp-body mx-4 text-sm md:mx-0 md:text-base">', request))==True:
                desc = request.split('<section class="adp-body mx-4 text-sm md:mx-0 md:text-base">')[1].split('</section>')[0]
                dfc.loc[index, 'full_description'] = desc
            else:
                dfc.loc[index,'full_description'] = 'not available'
        else:
            dfc.loc[index,'full_description'] = 'fetching error'

    sparkDF = spark.createDataFrame(dfc)
    df_replace = sparkDF.withColumn("full_description",
                               regexp_replace("full_description", """<(?!\/?a(?=>|\s.*>))\/?.*?>""", ""))
    return df_replace


def appendToCSV(df):
    with open('jobs.csv', 'a') as f:
        df.to_csv(f, header=False)
    f.close()
    # with open('jobs.csv', 'a', newline='') as f_object:
    #     # Pass the CSV  file object to the writer() function
    #     writer_object = writer(f_object)
    #     # Result - a writer object
    #     # Pass the data in the list as an argument into the writerow() function
    #     writer_object.writerow(df_row)
    #     # Close the file object
    #     f_object.close()

url = 'https://api.adzuna.com/v1/api/jobs/us/search/'
app_key = 'db9ca784da33298ade0ae4bb73ea9be8'
app_id = 'e27c0930'
page = 1

df = pd.DataFrame(columns=['title', 'contract_type', 'description',
                  'salary_max', 'salary_min', 'location.display_name', 'salary_is_predicted', 'redirect_url','created','category.tag','full_description'])

while page > 0:
    resp = getPayload(app_id=app_id,app_key=app_key,url=url,page =page,results_per_page=100,salary_min=1,full_time=1)
    if bool(resp.empty) ==False:
        api_df = load_data(resp)
        df = df.append(getFullDesc(api_df), ignore_index=True)
        # Create PySpark DataFrame from Pandas

        print(df)
        #appendToCSV(df_replace)
        print(page)
        page = page + 1
    else:
        break
#df.to_csv('data.csv')




#rdd=sc.parallelize(resp.text)
#df = spark.read.json(rdd)#json_df.show()
#print(rdd.collect())

# from pyspark.sql.types import MapType,StringType
# from pyspark.sql.functions import from_json
# df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
# df2.printSchema()
# df2.show(truncate=False)

# from pyspark.sql import SparkSession,Row
# spark = SparkSession.builder.appName('CareerPlatform').getOrCreate()
# df = spark.read.json("data/adzuna_job_search-1652552172421.json")
# #df.show(truncate=False)








