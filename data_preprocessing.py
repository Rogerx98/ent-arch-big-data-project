import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import json
import findspark
import pandas as pd
findspark.init()

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import regexp_replace


spark = SparkSession.builder \
    .master("local[1]") \
    .appName("careerApp") \
    .getOrCreate() \
    #.config("spark.driver.memory", "15g") \

def getPayload(url,app_id,app_key='', search_key='data',page=1,results_per_page=1000,salary_min=1,full_time=1):

#define request parameters
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

#if the request response with 200 success then fetch the JSON payload
    if resp.status_code==200:
        data = json.loads(resp.text)
        print(data)
        print(resp.status_code)
        result = pd.json_normalize(data, record_path=["results"])
#if response is not successful, return status code
    else:
        print('Error status code'+ ' '+str(resp.status_code))
    return result

#
def load_data(data):
    properties = ['title','company.display_name' ,
                  'salary_max', 'salary_min', 'location.display_name', 'salary_is_predicted', 'redirect_url','created','category.label']

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
    dfc.rename(columns = {'company.display_name':'company','location.display_name':'location'}, inplace = True)

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
    df_temp = sparkDF.withColumn("full_description",
                               regexp_replace("full_description", """<(?!\/?a(?=>|\s.*>))\/?.*?>""", ""))
    df_replace = df_temp.withColumn("full_description",
                                    regexp_replace("full_description", """[ ^ >]+""", " "))
    df_replace.write.format("json").mode("append").save("hdfs://seneca1.lehre.hwr-berlin.de:8020/user/ngoc_nguyen/career_app")

    df_replace.show()

url = 'https://api.adzuna.com/v1/api/jobs/us/search/'
app_key = 'db9ca784da33298ade0ae4bb73ea9be8'
app_id = 'e27c0930'
page = 1

while page >0:
    resp = getPayload(app_id=app_id,app_key=app_key,url=url,page =page,results_per_page=100,salary_min=1,full_time=1)
    if bool(resp.empty) ==False:
        api_df = load_data(resp)
        spark_df = getFullDesc(api_df)
        page = page + 1

    else:
        break