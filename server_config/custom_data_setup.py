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
    data = pd.read_csv("organisations.csv") 
    organisation_id = list(data['id'])
    lastname = list(map(str, list(data['lastname'])))
    edate = list(map(str, list(data['edate'])))
    twitter = list(map(str, list(data['twitter'])))
    link = list(map(str, list(data['link'])))
    incdate = list(map(str, list(data['incdate'])))
    cdate = list(map(str, list(data['cdate'])))
    creason = list(map(str, list(data['creason'])))
    creasonx = list(map(str, list(data['creasonx'])))
    cryptonative = list(map(str, list(data['cryptonative'])))
    verified = list(map(str, list(data['verified'])))
    comments = list(map(str, list(data['comments'])))
    c.execute("CREATE TABLE IF NOT EXISTS organisations (id INT PRIMARY KEY, lastname VARCHAR(255) NOT NULL, edate DATE, twitter VARCHAR(63), link VARCHAR(255), incdate DATE, cdate VARCHAR(10), creason VARCHAR(255), creasonx VARCHAR(3), cryptonative VARCHAR(63), verified BOOL, comments VARCHAR(2000));")
    insert_sql = "INSERT INTO organisations (id, lastname, edate, twitter, link, incdate, cdate, creason, creasonx, cryptonative, verified, comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    for item in zip(organisation_id, lastname, edate, twitter, link, incdate, cdate, creason, creasonx, cryptonative, verified, comments):
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
