# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 21:52:35 2019
@author: Anton
"""
import psycopg2
import yaml
import pandas as pd
from sqlalchemy import create_engine

# Reading CONFIG file with DB credentials
config_path = '../CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}
db_url = 'postgresql://'+config['atlas']['user']+":"+config['atlas']['password']+"@"+config['atlas']['host']+":"+str(config['atlas']['port'])+"/"+config['atlas']['dbname']
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
    c.execute("CREATE TABLE IF NOT EXISTS countries (id INT PRIMARY KEY, code CHAR(2) NOT NULL, name VARCHAR(63) NOT NULL, flag VARCHAR(255));")
    c.execute("CREATE TABLE IF NOT EXISTS taxonomy (id INT PRIMARY KEY, segment VARCHAR(140), description VARCHAR(255), "
          "s_id INT, category VARCHAR(140), c_id INT, subcategory VARCHAR(140), b_id INT);")
    c.execute("CREATE TABLE IF NOT EXISTS organisations (id INT PRIMARY KEY, lastname VARCHAR(255) NOT NULL, "
          "edate DATE NOT NULL, twitter VARCHAR(63), link VARCHAR(255), incdate DATE, cdate DATE, creason VARCHAR(255), creasonx VARCHAR(1), "
          "cryptonative VARCHAR(63), verified BOOL, comments VARCHAR(2000));")
    c.execute("CREATE TABLE IF NOT EXISTS organisation_states (id INT PRIMARY KEY, organisation_id INT REFERENCES organisations(id), "
          "year INT NOT NULL, name VARCHAR(255) NOT NULL, type VARCHAR(63), "
          "incnum VARCHAR(63), ophqcity VARCHAR(123), ophq INT REFERENCES countries(id), "
          "leghqcity VARCHAR(123), leghq INT REFERENCES countries(id), arbjur VARCHAR(123), "
          "description VARCHAR(511), fte INT);")
    c.execute("CREATE TABLE IF NOT EXISTS categories (id INT PRIMARY KEY, organisation_state_id INT REFERENCES organisation_states(id), "
          "identifier INT REFERENCES taxonomy(id));")
    
# Filling in countries table from countries.csv
countries_data = pd.read_csv("countries.csv") 
countries_data.to_sql('countries', con=engine, if_exists='append', index=False)

# Filling in taxonomy table from taxonomy.csv
taxonomy_data = pd.read_csv("taxonomy.csv") 
taxonomy_data.to_sql('taxonomy', con=engine, if_exists='append', index=False)

#===================== TEMRORARY FILLING IN BELOW =============================
organisations_data = pd.read_csv("organisations.csv") 
organisations_data.to_sql('organisations', con=engine, if_exists='append', index=False)
organisation_states_data = pd.read_csv("organisation_states.csv") 
organisation_states_data.to_sql('organisation_states', con=engine, if_exists='append', index=False)
categories = pd.read_csv("categories.csv") 
categories.to_sql('categories', con=engine, if_exists='append', index=False)

# =============================================================================
#     insert_sql = "INSERT INTO source_table (id, name, edate, ctype, twitter, link, incdate, incnum, " \
#                  "cdate, creason, creasonx, ophqcity, ophq, leghqcity, leghq, arbjur, " \
#                  "cryptonative, classifier, description, fte, verified, comments) " \
#                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
#     for item in zip(d["id"], d["name"], d["edate"], d["ctype"], d["twitter"], d["link"], d["incdate"], d["incnum"],
#                     d["cdate"], d["creason"], d["creasonx"], d["ophqcity"], d["ophq"], d["leghqcity"], d["leghq"], d["arbjur"],
#                     d["cryptonative"], d["classifier"], d["description"], d["fte"], d["verified"], d["comments"]):
#         c.execute(insert_sql, item)
# =============================================================================