# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 21:52:35 2019

@author: Anton
"""

import psycopg2
import yaml
import pandas as pd
from sqlalchemy import create_engine

config_path = '../CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}


with psycopg2.connect(**config['atlas']) as connection:
    c = connection.cursor()
    data = pd.read_csv("source_table.csv") 
    c.execute("CREATE TABLE IF NOT EXISTS source_table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, "
              "edate DATE NOT NULL, ctype VARCHAR(255), twitter VARCHAR(63), link VARCHAR(255), incdate DATE, "
              "incnum VARCHAR(255), cdate DATE, creason VARCHAR(255), creasonx VARCHAR(3), "
              "ophqcity VARCHAR(999), ophq VARCHAR(511), "
              "leghqcity VARCHAR(999), leghq VARCHAR(511), arbjur VARCHAR(999), "
              "cryptonative VARCHAR(63), classifier VARCHAR(2000), description VARCHAR(2000), "
              "fte VARCHAR(511), verified BOOL, comments VARCHAR(2000));")
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
        
db_url = 'postgresql://'+config['atlas']['user']+":"+config['atlas']['password']+"@"+config['atlas']['host']+":"+str(config['atlas']['port'])+"/"+config['atlas']['dbname']
engine = create_engine(db_url)
data.to_sql('source_table', con=engine, if_exists='append', index=False)