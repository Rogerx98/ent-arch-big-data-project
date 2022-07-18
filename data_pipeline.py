import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import json
import findspark
import pandas as pd
findspark.init()
from pyspark.sql.functions import udf, col, explode, lit, monotonically_increasing_id, row_number, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql import Row, Window
import pyspark
from pyspark.context import SparkContext
import calendar
import time
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import regexp_replace
from transformers import pipeline
#import spacy
#import spacy_transformers
import psycopg2
import psycopg2.extras

import datetime

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("careerApp") \
    .getOrCreate() \
    #.config("spark.driver.memory", "15g") \


def classifyData(df,seniority_db_df, career_db_df,skills_df,city_db_df,company_db_df):

    #define the career labels
    candidate_career = ["data scientist", "data engineer", "data analyst", "business intelligence consultant",
                               "data product manager", "data architect", "software engineer",
                               "data warehouse specialist", "business admin", "data product manager"]

    # define the seniority labels
    candidate_seniority = ['senior', 'middle', 'junior']
    seniority = []
    career = []
    df_collect = df.collect()

    for row in df_collect:
        #classify seniority label for each job record
        seniority_result = classifier(row['full_description'], candidate_seniority)
        max_idx_sen = np.argmax(seniority_result["scores"])
        seniority.append(seniority_result["labels"][max_idx_sen])

        # classify career label for each job record
        career_result = classifier(row['full_description'], candidate_career)
        max_idx_career = np.argmax(career_result["scores"])
        career.append(career_result["labels"][max_idx_career])

    #create separate dataframes for classification labels
    sen_df = spark.createDataFrame([(s,) for s in seniority], ['seniority'])
    career_df = spark.createDataFrame([(c,) for c in career], ['career'])
    df = df.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id()))).repartition(4)
    sen_df = sen_df.withColumn("row_idx_sen", row_number().over(Window.orderBy(monotonically_increasing_id()))).repartition(4)
    career_df = career_df.withColumn("row_idx_career", row_number().over(Window.orderBy(monotonically_increasing_id()))).repartition(4)
    #join the main dataframe with the classification labels
    temp_df = df.join(sen_df, df.row_idx == sen_df.row_idx_sen).repartition(4)
    temp_df_1 = temp_df.join(career_df, temp_df.row_idx == career_df.row_idx_career).repartition(4)
    # match the corresponding id of the seniority label
    temp_df_2 = temp_df_1.join(seniority_db_df,temp_df_1.seniority == seniority_db_df.seniority_desc)

    # match the corresponding id of the company
    temp_df_3 = temp_df_2.join(company_db_df, temp_df_2.company == company_db_df.company_name)

    # match the corresponding id of the career label
    final_df = temp_df_3.join(career_db_df,temp_df_3.career == career_db_df.career_desc)
    return final_df

def get_db_data():
    # establishing the connection
    conn = psycopg2.connect(
        database="career_platform", user='consultant', password='b1pmmasterBERLIN', host='10.50.200.15', port='5432'
    )
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    # get all data from the skill dimension table
    cursor.execute("select * from skill")
    skills_row = Row("skill_id", "skill")
    row_skill_data = []
    for skill_id, skill in cursor.fetchall():
        row_skill_data.append(skills_row(skill_id, skill))
    skills_df = spark.createDataFrame(row_skill_data)

    # get all data from the seniority dimension table
    cursor.execute("select * from seniority")
    seniority_row = Row("seniority_id", "seniority_desc")
    row_sen_data = []
    for seniority_id, seniority in cursor.fetchall():
        row_sen_data.append(seniority_row(seniority_id, seniority))
    seniority_df = spark.createDataFrame(row_sen_data)

    # get all data from the career dimension table
    cursor.execute("select * from career")
    career_row = Row("career_id", "career_desc")
    row_career_data = []
    for career_id, career in cursor.fetchall():
        row_career_data.append(career_row(career_id, career))
    career_df = spark.createDataFrame(row_career_data)

    # get all data from the city dimension table
    cursor.execute("select * from city")
    city_row = Row("city_id", "city_name")
    row_city_data = []
    for city_id, city_name in cursor.fetchall():
        row_city_data.append(city_row(city_id,city_name))
    city_df = spark.createDataFrame(row_city_data)
    #city_df.show()

    # get all data from the company dimension table
    cursor.execute("select * from company")
    company_row = Row("company_id", "company_name")
    row_company_data = []
    for company_id, company_name in cursor.fetchall():
        row_company_data.append(company_row(company_id,company_name))
    company_df = spark.createDataFrame(row_company_data)

    # Closing the connection
    conn.close()
    return skills_df ,seniority_df, career_df, city_df, company_df


def match_city(city_db_df, spark_df):
    df = spark_df.toPandas()
    df["location"] = df["location"].str.split(",").str[0]
    df.location
    df_spark = spark.createDataFrame(df)
    final_df = df_spark.join(city_db_df, df_spark.location == city_db_df.city_name)
    #final_df.show()
    return final_df

def matchSkills(skills_df,final_df):
    pd_df = final_df.toPandas()
    skills_df_collect = skills_df.collect()

    #find if a skill existing in the skill dimension table match with any keywords in the job description
    for a_skill in skills_df_collect:
        #pd_df = df.toPandas()
        pd_df["skill_id"] = a_skill['skill_id']
        sp_new_df = spark.createDataFrame(pd_df)
        results = sp_new_df.filter(sp_new_df.full_description.contains(a_skill['skill']))
        results.createOrReplaceTempView('fact_table')
        final_results = spark.sql('SELECT skill_id, career_id,city_id,company_id,created,salary_max,seniority_id,title FROM fact_table')

        # insert the dataframe to Postgres Data warehouse
        insertIntoTable(final_results, 'career_skill')



#insert final dataframe to the fact table
def insertIntoTable(spark_df, table):
    df = spark_df.toPandas()
    df["city_id"] = pd.to_numeric(df["city_id"])
    df["created"] = pd.to_numeric(df["created"])

    # establishing the connection
    conn = psycopg2.connect(
        database="career_platform", user='consultant', password='b1pmmasterBERLIN', host='10.50.200.15', port='5432'
    )
    # Creating a cursor object using the cursor() method
    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        conn.commit()
        cur.close()

#convert created date into time_id
def extractDateID(spark_df):
    pd_df = spark_df.toPandas()
    for indx, row in pd_df.iterrows():
        temp = str(row['created'])[0:7]
        pd_df.loc[indx,'created'] = temp.replace('-', '')
    spark_final = spark.createDataFrame(pd_df)
    #spark_df.show()
    return spark_final

#save new city into the dimension table 'city'
def store_city(spark_df,city_db_df):
    df = spark_df.toPandas()
    df["city"] = df["location"].str.split(",").str[0]

    #get a list of unique city names in the new dataframe
    df_unique = df["city"].unique()
    df_u = pd.DataFrame(df_unique)

    #save the new cities if the existing 'city' dimension table does not have any similar city name
    for indx,row in df_u.iterrows():
        if city_db_df.filter(col("city_name").like(df_u.loc[indx,0])).count()==0:
            conn = psycopg2.connect(
            database="career_platform", user='consultant', password='b1pmmasterBERLIN', host='10.50.200.15', port='5432'
        )
            df_columns = list(df_u)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format('city', 'name', values)

            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df_u.values)
            conn.commit()
            cur.close()
        else:
            continue

#save new company into the dimension table 'company'
def storeCompany(spark_df,company_db_df):
    df = spark_df.toPandas()
    #print(df["company"].unique())
    df_unique = df["company"].unique()
    df_u = pd.DataFrame(df_unique)

    #save the new companies if the existing 'company' dimension table does not have any similar company name
    for indx,row in df_u.iterrows():
        if company_db_df.filter(col("company_name").like(df_u.loc[indx,0])).count()==0:
            conn = psycopg2.connect(
            database="career_platform", user='consultant', password='b1pmmasterBERLIN', host='10.50.200.15', port='5432'
        )
            df_columns = list(df_u)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format('company', 'company', values)

            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df_u.values)
            conn.commit()
            cur.close()

master_schema = StructType([
  StructField('title', StringType(), True),
  StructField('company', StringType(), True),
  StructField('salary_max', FloatType(), True),
  StructField('salary_min', FloatType(), True),
  StructField('location', StringType(), True),
  StructField('salary_is_predicted', StringType() , True),
  StructField('redirect_url', StringType() , True),
  StructField('created',DateType(), True),
  StructField('category.label',StringType(), True),
  StructField('full_description',StringType(), True)
  ])
skill_schema = StructType([
  StructField('skill_id', IntegerType(), True),
  StructField('skill_desc', StringType(), True)
])

seniority_schema = StructType([
  StructField('seniority_id', IntegerType(), True),
  StructField('seniority_desc', StringType(), True)
])

company_schema = StructType([
  StructField('company_id', IntegerType(), True),
  StructField('company_name', StringType(), True)
])

career_schema = StructType([
  StructField('career_id', IntegerType(), True),
  StructField('career', StringType(), True)
])

city_schema = StructType([
  StructField('city_id', IntegerType(), True),
  StructField('city_name', StringType(), True)
])

spark_df = spark.createDataFrame([],master_schema)
skills_df = spark.createDataFrame([],skill_schema)
seniority_df = spark.createDataFrame([],seniority_schema)
career_df = spark.createDataFrame([],career_schema)
city_df = spark.createDataFrame([],city_schema)
company_df = spark.createDataFrame([],company_schema)

classifier = pipeline("zero-shot-classification",
                          model="facebook/bart-large-mnli")

spark_df=spark.read.json("hdfs://seneca1.lehre.hwr-berlin.de:8020/user/ngoc_nguyen/career_app", schema=master_schema)

#spark_df = spark_df.limit(5)
#get all data from the dimension tables
skills_df, seniority_df,career_df,city_df, company_df = get_db_data()

#if there is any new city that does not exist in the city table, save the new cities
store_city(spark_df,city_df)

#if there is any new company that does not exist in the company table, save the new companies
storeCompany(spark_df,company_df)

#fetch data from dimension tables again to get the latest version
new_skills_df, new_seniority_df,new_career_df,new_city_df, new_company_df = get_db_data()

#match the city name with existing city_id
city_matched_df = match_city(new_city_df, spark_df)

#convert created date into the time_id format
date_matched_df = extractDateID(city_matched_df)

#create data pipeline and do ETL
classified_df = classifyData(date_matched_df, new_seniority_df,new_career_df, new_skills_df,new_city_df,new_company_df)

#match job skills with existing skill_id and save the complete dataframe to the fact table
matchSkills(new_skills_df, classified_df)







