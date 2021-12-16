from gssutils import *
import json 
import requests
import pandas as pd
import numpy as np


metadata = Scraper(seed="info.json")
# metadata.as_csvqb_catalog_metadata()

# +
# datasetTitle = scraper.title
# datasetTitle = "UK trade in goods by business characteristics - data tables"
# -

info = json.load(open('info.json'))

metadata.select_dataset(title = lambda x: x.endswith('data tables'), latest = True)
metadata.dataset.family = info["families"]

distribution = metadata.distribution(title = lambda t : 'data tables' in t, latest = True)
distribution

tabs = {tab.name: tab for tab in metadata.distribution(title = lambda t : 'data tables' in t).as_databaker()}
list(tabs)

# +
tidied_sheets = []
for name, tab in tabs.items():
    if 'Notes and Contents' in name or '5. Metadata' in name :
        continue
    # print(tab.name)

    cell = tab.excel_ref("A1")
    
    flow = tab.filter(contains_string("Flow")).fill(DOWN).is_not_blank().is_not_whitespace()

    year = tab.filter("Year").fill(DOWN).is_not_blank().is_not_whitespace()

    country = tab.filter(contains_string("Country")).fill(DOWN).is_not_blank().is_not_whitespace()

    zone = tab.filter(contains_string("Zone")).fill(DOWN).is_not_blank().is_not_whitespace()

    business_size = tab.filter(contains_string("Business Size")).fill(DOWN).is_not_blank().is_not_whitespace()
    age = tab.filter(contains_string("Age (Years)")).fill(DOWN).is_not_blank().is_not_whitespace()

    industry_group = tab.filter(contains_string("Industry Group")).fill(DOWN).is_not_blank().is_not_whitespace()

    business_count = tab.filter(contains_string("Business Count")).fill(DOWN).is_not_blank().is_not_whitespace()

    employee_count = tab.filter(contains_string("Employee Count")).fill(DOWN).is_not_blank().is_not_whitespace()

    observations = cell.shift(7,2).fill(DOWN).is_not_blank().is_not_whitespace()
    
    dimensions = [
        HDim(flow, 'Flow', DIRECTLY, LEFT),
        HDim(year, 'Period', DIRECTLY, LEFT),
        HDim(country, 'Country', DIRECTLY, LEFT),
        HDim(zone, 'Zone', DIRECTLY, LEFT),
        HDim(business_size, 'Business Size', DIRECTLY, LEFT),
        HDim(age, 'Age', DIRECTLY, LEFT),
        HDim(industry_group, 'Industry Group', DIRECTLY, LEFT),
        HDim(business_count, 'Business Count', DIRECTLY, RIGHT),
        HDim(employee_count, 'Employee Count', DIRECTLY, RIGHT),
    ]
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    # savepreviewhtml(year, fname = tab.name+ "Preview.html")
    tidied_sheets.append(tidy_sheet.topandas())



# -

df = pd.concat(tidied_sheets, sort = True)
df.rename(columns= {'OBS':'Value', 'Period':'Year', 'Flow':'Flow Directions', 'DATAMARKER':'Marker'}, inplace = True)
df = df.fillna('unavailable')
df

df["Employee Count"] = df["Employee Count"].apply(lambda x: str(x).split(".")[0])
df["Business Count"] = df["Business Count"].apply(lambda x:str(x).split(".")[0])

df['Marker'] = df['Marker'].replace('Suppressed', 'suppressed', regex=True)
# df['Marker'] = df['Marker'].fillna('unavailable')

def left(s,amount):
    return s[:amount]
def right(s,amount):
    return s[-amount:]
def date_time(date):
    if len(date) == 5:
        return left(date, 4)
df['Year'] = df['Year'].astype(str).replace('\.', '', regex=True)
df['Year'] = df['Year'].apply(date_time)

df['Marker'] = df['Marker'].map(lambda m: None if m == 'unavailable' else m)
df['Employee Count'] = df['Employee Count'].map(lambda ec: None if ec == 'n/a' else ec)

df = df.rename(columns={'Flow Directions': "Flow", "Business Size": "Number of Employees", "Age": "Age of Business"})
df['Age of Business'] = df['Age of Business'].map(pathify)

cl_area_mappings = pd.read_csv("https://raw.githubusercontent.com/GSS-Cogs/ref_common/pmd4/reference/codelists/cl-area.csv")
map_cl_area_label_to_notation = { l: n for l, n in zip(list(cl_area_mappings["Label"]), list(cl_area_mappings["Notation"]))}

df = df.drop(df.index[df['Country'] == "Non-EU"])
df = df.drop(df.index[df['Zone'] == "Non-EU"])

cl_label_match_replacements = {
    "Hong Kong": "Hong Kong, China",
    "Russia": "Russian Federation",
    "South Korea": "Korea, Republic of",
    "Taiwan": "Taiwan, Province of China",
    "UAE": "United Arab Emirates",
    "Vietnam": "Viet Nam",
    "EU": "EU (Member States and Institutions of the European Union) changing composition",
    "World": "World (all areas, including reference area, including IO)"
}

df['Country'] = df['Country'].replace(cl_label_match_replacements)
df['Country'] = df['Country'].map(lambda c: map_cl_area_label_to_notation[c])

df['Zone'] = df['Zone'].replace(cl_label_match_replacements)
df['Zone'] = df['Zone'].map(lambda c: map_cl_area_label_to_notation[c])
df['Industry Group'] = df['Industry Group'].map(pathify)
df['Number of Employees'] = df['Number of Employees'].map(pathify)


df['Flow'] = df['Flow'].map(pathify)
df['Flow'].loc[(df['Flow'] == 'import')] = 'imports'
df['Flow'].loc[(df['Flow'] == 'export')] = 'exports'
df['Value'].loc[(df['Value'] == '')] = 0
df['Value'] = df['Value'].astype(int)



with pd.option_context('float_format', '{:f}'.format):
    print(df)

df.to_csv("observations.csv", index = False)
catalog_metadata = metadata.as_csvqb_catalog_metadata()
catalog_metadata.to_json_file('catalog-metadata.json')