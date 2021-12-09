import json 
import pandas as pd

url = "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2020-financial-year-provisional/Download-data/annual-enterprise-survey-2020-financial-year-provisional-csv.csv"

df = pd.read_csv(url)

df.columns = [x.lower() for x in df.columns]

tidy_df = df[['year', 'industry_code_anzsic06', 'industry_code_nzsioc', 'variable_name','value', 'units']]

tidy_df["industry_code_anzsic06"] = tidy_df["industry_code_anzsic06"].apply(lambda x: x.lower())
tidy_df["industry_code_nzsioc"] = tidy_df["industry_code_nzsioc"].apply(lambda x: x.lower())
tidy_df["variable_name"] = tidy_df["variable_name"].apply(lambda x: x.lower())

tidy_df.head(62).to_csv("observations.csv", index=False)
