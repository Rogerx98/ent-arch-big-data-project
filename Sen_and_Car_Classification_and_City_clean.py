import pandas as pd
import re as re
import numpy as np
from transformers import pipeline

df = pd.read_csv("data copy.csv")
#df = df.sample(frac = 0.0010) #for tests with small sample (4 rows)
print(df.info())


#REMOVE HTML TAGS FROM FULL_DESCRIPTION
def remove_tags(string):
    result = re.sub('<.*?>','',string)
    return result
df['full_description']=df['full_description'].apply(lambda cw : remove_tags(cw))
print(df.info())

#SENIORITY CLASSIFICATION
classifier = pipeline("zero-shot-classification",
                      model="facebook/bart-large-mnli")

sequence_to_classify_seniority = df["full_description"]
candidate_labels_seniority = ['senior', 'middle', 'junior']

labels_seniority_forloop = []
for desc in df["full_description"]:
    result = classifier(desc, candidate_labels_seniority)
    max_idx = np.argmax(result["scores"])
    label = result["labels"][max_idx]
    labels_seniority_forloop.append(label)

#ATTACH TO WHOLE DF
df["seniority_label"] = labels_seniority_forloop




#CAREER CLASSIFICATION

sequence_to_classify_career = df["Unnamed: 0.1"]
candidate_labels_career = ["data scientist", "data engineer", "data analyst", "business intelligence consultant", "data product manager", "data architect", "software engineer", "data warehouse specialist", "business manager", "data product manager"]

labels_career_forloop = []
for desc in df["Unnamed: 0.1"]:
    result = classifier(desc, candidate_labels_career)
    max_idx = np.argmax(result["scores"])
    label = result["labels"][max_idx]
    labels_career_forloop.append(label)

#ATTACH TO WHOLE DF
df["career_label"] = labels_career_forloop
print(df)

#index = torch.argmax(result["scores"])
#label = result["labels"][index]

#CITY CLEANING

df["location.display_name"] = df["location.display_name"].str.split(",").str[0]

df.to_csv("Seniority_and_career_pred.csv")