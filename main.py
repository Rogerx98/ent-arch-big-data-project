import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt

st.title("Career Orientation Platform")
st.write("Filter and get career insight")

exec(open("script.py").read())

options = st.multiselect(
     'Select careers that you are interested in:',
     ['Data Analyst', 'Data Engineer','Business Intelligence','Data Scientist'],
     ['Business Intelligence','Data Scientist','Data Analyst','Data Engineer'])

fig, ax = plt.subplots()

for option in options:
    avg_salary = df[df['title'].str.contains(option, na=True) == True]['salary_max'].dropna().mean()
    ax.bar(option,avg_salary)

st.pyplot(fig)













