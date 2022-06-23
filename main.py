#import streamlit as st
import numpy as np
import pandas as pd
#import plotly.express as px
#import plotly.graph_objects as go
#import matplotlib.pyplot as plt
import os
import json
# st.title("Career Orientation Platform")
# st.write("Filter and get career insight")
#
# exec(open("script.py").read())

def load_data():
    properties = ['title', 'contract_type', 'description',
                  'salary_max', 'salary_min', 'location.display_name', 'salary_is_predicted', 'redirect_url','created']
    directory = 'data'
    df = pd.DataFrame(columns=[p for p in properties])

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file = open(directory + '/' + filename)
            content = json.load(file)
            print(type(content))
            data = pd.json_normalize(content, ["results"])
            for p in properties:
                if p in (data.columns.values):
                    continue
                else:
                    data[p] = np.nan
            data = data[[p for p in properties]]
            df = df.append(data, ignore_index=True)
            file.close()
        else:
            continue
    return df

df = load_data()

# df.to_csv('data.csv')
#
# options = st.multiselect(
#      'Select careers that you are interested in:',
#      ['Data Analyst', 'Data Engineer','Business Intelligence','Data Scientist'],
#      ['Business Intelligence','Data Scientist','Data Analyst','Data Engineer'])
#
# fig, ax = plt.subplots()
#
# for option in options:
#     avg_salary = df[df['title'].str.contains(option, na=True) == True]['salary_max'].dropna().mean()
#     ax.bar(option,avg_salary)
#
# st.pyplot(fig)













