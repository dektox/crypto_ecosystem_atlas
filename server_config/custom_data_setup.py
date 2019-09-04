# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 21:52:35 2019

@author: Anton
"""

import psycopg2
import yaml
import pandas as pd

config_path = '../CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}

conn = psycopg2.connect(**config['atlas'])
c = conn.cursor()


with psycopg2.connect(**config['atlas']) as connection:
    c = connection.cursor()
    d = pd.read_csv("source_table.csv") 
    c.execute("CREATE TABLE IF NOT EXISTS source_table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, "
              "edate VARCHAR(10), ctype VARCHAR(255), twitter VARCHAR(63), link VARCHAR(255), incdate VARCHAR(10), "
              "incnum VARCHAR(255), cdate VARCHAR(10), creason VARCHAR(255), creasonx VARCHAR(3), "
              "ophqcity VARCHAR(999), ophq VARCHAR(511) NOT NULL, "
              "leghqcity VARCHAR(999), leghq VARCHAR(511) NOT NULL, arbjur VARCHAR(999), "
              "cryptonative VARCHAR(63), classifier VARCHAR(2000), description VARCHAR(2000), "
              "fte VARCHAR(511), verified BOOL, comments VARCHAR(2000));")
    insert_sql = "INSERT INTO source_table (id, name, edate, ctype, twitter, link, incdate, incnum, " \
                 "cdate, creason, creasonx, ophqcity, ophq, leghqcity, leghq, arbjur, " \
                 "cryptonative, classifier, description, fte, verified, comments) " \
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    for item in zip(d["id"], d["name"], d["edate"], d["ctype"], d["twitter"], d["link"], d["incdate"], d["incnum"],
                    d["cdate"], d["creason"], d["creasonx"], d["ophqcity"], d["ophq"], d["leghqcity"], d["leghq"], d["arbjur"],
                    d["cryptonative"], d["classifier"], d["description"], d["fte"], d["verified"], d["comments"]):
        c.execute(insert_sql, item)


# =============================================================================
# with psycopg2.connect(**config['atlas']) as c:
#     data = pd.read_csv("categories.csv") 
#     categories_id = list(data['id'])
#     organisation_state_id = list(data['organisation_state_id'])
#     identifier = list(data['identifier'])
#     c.execute("CREATE TABLE IF NOT EXISTS categories (id INT PRIMARY KEY, organisation_state_id INT FOREIGN KEY, identifier INT FOREIGN KEY);")
#     insert_sql = "INSERT INTO countries (categories_id, organisation_state_id, identifier) VALUES (%s, %s, %s);"
# 
# =============================================================================
# =============================================================================
# import csv
# from collections import defaultdict
# 
# columns = defaultdict(list) # each value in each column is appended to a list
# 
# with open('countries.csv') as f:
#     reader = csv.DictReader(f) # read rows into a dictionary format
#     for row in reader: # read a row as {column1: value1, column2: value2,...}
#         for (k,v) in row.items(): # go over each column name and value 
#             columns[k].append(v) # append the value into the appropriate list
#                                  # based on column name k
# 
# country=columns['country']
# code=columns['code']
# electricity_consumption=columns['electricity_consumption']
# country_flag=columns['country_flag']
# =============================================================================
