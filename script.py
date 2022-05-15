import json
import os
import pandas as pd
import numpy as np

def load_data():
    properties = ['title', 'contract_type', 'description',
                  'salary_max', 'salary_min', 'location.display_name', 'salary_is_predicted', 'redirect_url','created']
    directory = 'data'
    df = pd.DataFrame(columns=[p for p in properties])

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file = open(directory + '/' + filename)
            content = json.load(file)
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