# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 21:52:35 2019
@author: Anton
"""
import psycopg2
import yaml
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime as dt
import numpy as np

# Reading CONFIG file with DB credentials
config_path = '../CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp)
       # config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}
db_url = 'postgresql://'+config['atlas']['user']+":"+config['atlas']['password']+"@"+config['atlas']['host']+":"\
         +str(config['atlas']['port'])+"/"+config['atlas']['dbname']
engine = create_engine(db_url)

# Creating source_table schema
with psycopg2.connect(**config['atlas']) as connection:
    c = connection.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS source_table (id INT PRIMARY KEY, name VARCHAR(999) NOT NULL, "
              "edate DATE NOT NULL, ctype VARCHAR(255), twitter VARCHAR(63), link VARCHAR(255), incdate DATE, "
              "incnum VARCHAR(255), cdate DATE, creason VARCHAR(255), creasonx VARCHAR(1), "
              "ophqcity VARCHAR(999), ophq VARCHAR(511), "
              "leghqcity VARCHAR(999), leghq VARCHAR(511), arbjur VARCHAR(999), "
              "cryptonative VARCHAR(63), classifier VARCHAR(2000), description VARCHAR(2000), "
              "fte VARCHAR(511), verified BOOL, comments VARCHAR(2000));")
    
# Filling in source_table from source_table.csv
source_data = pd.read_csv("source_table.csv") 
source_data.to_sql('source_table', con=engine, if_exists='append', index=False)

# Creating schemas of all other tables
with psycopg2.connect(**config['atlas']) as connection:
    c = connection.cursor()
    c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    c.execute("CREATE TABLE IF NOT EXISTS countries (id INT PRIMARY KEY, code CHAR(2) NOT NULL, "
              "name VARCHAR(63) NOT NULL, flag VARCHAR(255));")
    c.execute("CREATE TABLE IF NOT EXISTS organisations (id INT PRIMARY KEY, "
              "edate DATE NOT NULL, twitter VARCHAR(63), link VARCHAR(255), incdate DATE, cdate DATE, creason "
              "VARCHAR(255), creasonx VARCHAR(1), cryptonative VARCHAR(63), verified BOOL, comments VARCHAR(2000));")
    c.execute("CREATE TABLE IF NOT EXISTS organisation_states (id INT PRIMARY KEY, organisation_id INT "
              "REFERENCES organisations(id), year INT NOT NULL, name VARCHAR(255) NOT NULL, ctype VARCHAR(63), "
              "incnum VARCHAR(63), ophqcity VARCHAR(123), ophq INT REFERENCES countries(id), "
              "leghqcity VARCHAR(123), leghq INT REFERENCES countries(id), arbjur VARCHAR(123), "
              "description VARCHAR(999), fte INT);")
    c.execute("CREATE TABLE IF NOT EXISTS segments (id INT PRIMARY KEY, "
              "segment VARCHAR(123), description VARCHAR(255));")
    c.execute("CREATE TABLE IF NOT EXISTS subsegments (id INT PRIMARY KEY, "
              "seg_id INT REFERENCES segments(id), subsegment VARCHAR(123));")
    c.execute("CREATE TABLE IF NOT EXISTS categories (id INT PRIMARY KEY, "
              "subseg_id INT REFERENCES subsegments(id), category VARCHAR(123), examples VARCHAR(123));")
    c.execute("CREATE TABLE IF NOT EXISTS state_categories (id INT PRIMARY KEY, organisation_state_id INT "
              "REFERENCES organisation_states(id), identifier INT REFERENCES categories(id));")
    c.execute("CREATE VIEW all_data AS "
              "SELECT o.*, os.id AS os_id, os.year, os.name, os.ctype, os.incnum, os.ophqcity, os.ophq, os.leghqcity, "
              "os.leghq, os.arbjur, os.description, os.fte, cat.id as cat_id, cat.identifier "
              "FROM organisations AS o INNER JOIN organisation_states AS os "
              "ON os.organisation_id = o.id "
              "INNER JOIN state_categories AS cat "
              "ON cat.organisation_state_id = os.id;")
    c.execute("CREATE VIEW taxonomy AS "
              "SELECT seg.*, subseg.id AS subseg_id, subseg.subsegment, c.id AS c_id, c.category, c.examples "
              "FROM segments AS seg INNER JOIN subsegments AS subseg "
              "ON subseg.seg_id = seg.id "
              "INNER JOIN categories AS c "
              "ON c.subseg_id = subseg.id;")

# Filling in countries table from countries.csv
countries_data = pd.read_csv("countries.csv")
countries_data.to_sql('countries', con=engine, if_exists='append', index=False)

# ============ Filling in segments, subsegment, categories tables =============
segments_data = pd.read_csv("segments.csv")
segments_data.to_sql('segments', con=engine, if_exists='append', index=False)

subsegments_data = pd.read_csv("subsegments.csv") 
subsegments_data.to_sql('subsegments', con=engine, if_exists='append', index=False)

categories_data = pd.read_csv("categories.csv") 
categories_data.to_sql('categories', con=engine, if_exists='append', index=False)

# =============================================================================
# # ===================== TEMPORARY FILLING IN BELOW ============================
# organisations_data = pd.read_csv("organisations.csv") 
# organisations_data.to_sql('organisations', con=engine, if_exists='append', index=False)
# organisation_states_data = pd.read_csv("organisation_states.csv") 
# organisation_states_data.to_sql('organisation_states', con=engine, if_exists='append', index=False)
# state_categories_data = pd.read_csv("state_categories.csv") 
# state_categories_data.to_sql('state_categories', con=engine, if_exists='append', index=False)
# 
# =============================================================================
# =================== Filling in organisations_data ===========================
organisations_data = source_data[['id','edate','twitter','link','incdate','cdate','creason','creasonx','cryptonative','verified','comments']]
organisations_data.to_sql('organisations', con=engine, if_exists='append', index=False)

# =================== Filling in organisation_states ===========================
source_data = pd.read_csv("source_table.csv") 
sd=source_data
sd['edate'] = pd.to_datetime(sd['edate'])
sd['cdate'] = pd.to_datetime(sd['cdate'])
sd['cdate_filled'] = sd['cdate'].fillna(dt.now()) # meaning that the company exists at the time of code launch
sd['incdate'] = pd.to_datetime(sd['incdate'])
res = pd.DataFrame(columns=['id', 'organisation_id', 'year', 'name', 'ctype', 'incnum', 'ophqcity', 'ophq', 'leghqcity', 'leghq', 'arbjur', 'description', 'fte'])
# filling in 'name' column; initialising all the lines
for i, row in enumerate(sd['name']):
    years = list(range(int(sd['edate'][i].strftime("%Y")), int(sd['cdate_filled'][i].strftime("%Y"))+1))
    # if row doesn't contain neither \n nor semicolon:
    if '\n' and ':' not in sd['name'][i]:
        for year in years:
            res=res.append({"organisation_id":sd['id'][i], "year":year, "name":row.strip()}, ignore_index = True)
    else:
    # defining "company state changes" that are user-entered and delimited by \n
        states = sd['name'][i].strip().split("\n")
        change_years = []
        for state in states: change_years.append(int(state.split(':')[0][0:4]))
        #spliting list of years to monotone intervals:
        change_years_idx = [{y:x for x, y in enumerate(years)}.get(elem) for elem in change_years]
        intervals = [years[i : j] for i,j in zip(change_years_idx, change_years_idx[1:] + [None])]
        # within each interval the names are the same:
        for interval, state in zip (intervals, states):
            for year in interval:
                name = state.split(':')[1].strip().strip(';"')
                res=res.append({"organisation_id":sd['id'][i], "year":year, "name":name}, ignore_index = True)

# setting multi index for res DataFrame: combination of organsiation_id and year is unique     
res_idx = res.set_index(["organisation_id", "year"], verify_integrity=True)
res_idx_cat = res_idx.copy()

# filling in all other columns of organisation_states table
columns_to_fill = ['ctype','incnum','ophqcity','ophq','leghqcity','leghq','arbjur','description','fte','classifier']
for column in columns_to_fill:
    for i, org_id in enumerate(sd['id']):
        years = list(range(int(sd['edate'][i].strftime("%Y")), int(sd['cdate_filled'][i].strftime("%Y"))+1))
        # working out all other columns
        if sd[column][i] is not np.nan:
            if '\n' and ':' not in sd[column][i]:
               for year in years:
                   if column == 'ophq' or column == 'leghq':
                       res_idx.at[(org_id, year),column] = dict(zip(countries_data['name'], countries_data['id']))[sd[column][i]]
                   elif column == 'classifier':
                       res_idx_cat.at[(org_id, year),'classifier'] = sd[column][i]
                   else:
                       res_idx.at[(org_id, year),column] = sd[column][i]
            else:
                states = sd[column][i].strip().split("\n")
                change_years = []
                for state in states: 
                    try:
                        change_years.append(int(state.split(':')[0][0:4]))
                    except:
                        print("\nError occured at", org_id, year, column, "\nRaw text:", state)
                #spliting list of years to monotone intervals:
                change_years_idx = [{y:x for x, y in enumerate(years)}.get(elem) for elem in change_years]
                intervals = [years[i : j] for i,j in zip(change_years_idx, change_years_idx[1:] + [None])]
                # within each interval the *column* values are the same:
                for interval, state in zip (intervals, states):
                    for year in interval:
                        data = state.split(':')[1].strip().strip('";')
                        if column == 'ophq' or column == 'leghq':
                            res_idx.at[(org_id, year),column] = dict(zip(countries_data['name'], countries_data['id']))[data]
                        elif column == 'classifier':
                            res_idx_cat.at[(org_id, year),'classifier'] = data
                        else:
                            res_idx.at[(org_id, year),column] = data
                            
res_idx['id'] = np.arange(len(res_idx))
res_idx_cat['id'] = np.arange(len(res_idx_cat))
res_idx.to_sql('organisation_states', con=engine, if_exists='append', index=True)

# =================== Filling in state_categories ============================
res_state_cats = pd.DataFrame(columns=['id', 'organisation_state_id', 'identifier'])
for i, org_state_id in enumerate(res_idx_cat['id']):
    try:
        state_cats = res_idx_cat['classifier'][i].strip().split(",")
        for state_cat in state_cats:
            if int(state_cat) in categories_data['id'].tolist():
                res_state_cats = res_state_cats.append({"organisation_state_id":int(org_state_id), "identifier":int(state_cat)}, ignore_index = True)
            else:
                print (f"error {state_cat} is not in taxonomy")
    except: 
        print("Error in defining company category, check dates:",res_idx_cat['name'][i])
res_state_cats['id'] = np.arange(len(res_state_cats))
try:
    res_state_cats.to_sql('state_categories', con=engine, if_exists='append', index=False)
except Exception as error:
    print (f"error occured {error}")

