# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:24:16 2019

@author: Anton
"""
from flask import Flask, jsonify
from datetime import datetime
from datetime import date
import flask
import requests
import time
from flask_cors import CORS
import psycopg2
import yaml

config_path = '../CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}
    

# loading data in cache of each worker:
def load_data():
    with psycopg2.connect(**config['atlas']) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM countries')
        countries = c.fetchall()
        c.execute('SELECT * FROM segments')
        seg = c.fetchall()
        c.execute('SELECT * FROM subsegments')
        subseg = c.fetchall()
        c.execute('SELECT * FROM categories')
        cat = c.fetchall()
        c.execute('SELECT * FROM organisations')
        orgs = c.fetchall()
        c.execute('SELECT * FROM organisation_states')
        org_states = c.fetchall()
        c.execute('SELECT * FROM state_categories')
        states_cat = c.fetchall()
        c.execute('SELECT * FROM all_data')
        all_data = c.fetchall()
        c.execute('SELECT * FROM taxonomy')
        taxonomy = c.fetchall()
    return countries, seg, subseg, cat, orgs, org_states, states_cat, all_data, taxonomy


def send_err_to_slack(err, name):
    try: 
        headers = {'Content-type': 'application/json',}
        data = {"text":""}
        data["text"] = f"Getting {name} failed. It unexpectedly returned: " + str(err)[0:140]
        requests.post(config['webhook_err'], headers=headers, data=str(data))
    except:
        pass # not the best practice but we want API working even if Slack msg failed for any reason


app = Flask(__name__)
CORS(app)
app.config["DEBUG"] = False

# initialisation of cache vars:
countries, seg, subseg, cat, orgs, org_states, states_cat, all_data, taxonomy = load_data()
lastupdate = time.time()
cache = {}
# cache:


@app.before_request
def before_request():
    global lastupdate, countries, seg, subseg, cat, orgs, org_states, states_cat, all_data, taxonomy, cache
    if time.time() - lastupdate > 7200:
        try:
            countries, seg, subseg, cat, orgs, org_states, states_cat, all_data, taxonomy = load_data()
        except Exception as err:
            app.logger.exception(f"Getting data from DB err: {str(err)}")
            send_err_to_slack(err, 'DB')
        else:
            cache = {}
            lastupdate = time.time()


@app.route("/api/geo", methods=['GET', 'POST'])
def countries_atlas():
    response = []
    for item in countries:
         response.append({
             'id': item[0],
             'code': item[1],
             'name': item[2],
            })
    return jsonify(response)


@app.route("/api/segments", methods=['GET', 'POST'])
def segments():
    response = []
    for item in seg:
         response.append({
             'id': item[0],
             'seg': item[1],
             'desc': item[2],
            })
    return jsonify(response)


@app.route("/api/subsegments", methods=['GET', 'POST'])
def subsegments():
    response = []
    for item in subseg:
         response.append({
             'id': item[0],
             'subseg': item[2],
            })
    return jsonify(response)


@app.route("/api/categories", methods=['GET', 'POST'])
def categories():
    response = []
    for item in cat:
         response.append({
             'id': item[0],
             'cat': item[2],
            })
    return jsonify(response)


@app.route("/api/taxonomy", methods=['GET', 'POST'])
def taxonomy_func():
    response = []
    for item in taxonomy:
         response.append({
             'id': item[0],
             'seg': item[1],
             'subseg_id': item[3],
             'subseg': item[4],
             'c_id': item[5],
             'cat': item[6],
            })
    return jsonify(response)


@app.route("/api/all", methods=['GET', 'POST'])
def all_data_func():
    response = []
    for item in all_data:
         response.append({
             'id': item[0],
             'edate': str(item[1]),
             'cdate': str(item[5]),
             'name': item[13],
             'os_id': item[11],
             'type': str(item[14]).lower(),
             'year': item[12],
             'ophq': item[17],
             'leghq': item[19],
             'weight': 1,
             'identifier': item[24],
            })
    return jsonify(response)


@app.route("/api/getorg/<value>")
def get_org(value):
    try:
        org_id = int(value)
    except ValueError:
        return jsonify("company ID should be INT")

    with psycopg2.connect(**config['atlas']) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM all_data WHERE id = {}'.format(org_id))
        company = c.fetchall()
    list_of_identifiers = []
    list_of_years = []
    response = []
    for item in company:
        list_of_years.append(item[12])
    maxyear=max(list_of_years)
    for item in company:
        if item[12] == maxyear:
            list_of_identifiers.append(item[24])
            if str(item[5]) == "None":
                active = True
            else:
                active = False
            response = {
                 'id': item[0],
                 'edate': str(item[1]),
                 'twitter': item[2],
                 'link': item[3],
                 'incdate': str(item[4]),
                 'cdate': str(item[5]),
                 'creason': item[6],
                 'creasonx': item[7],
                 'cryptonative': item[8],
                 'os_id': item[11],
                 'year': item[12],
                 'name': item[13],
                 'type': str(item[14]).lower(),
                 'incnum': item[15],
                 'ophqcity': item[16],
                 'ophq': item[17],
                 'leghqcity': item[18],
                 'leghq': item[19],
                 'arbjur': item[20],
                 'desc': item[21],
                 'fte': item[22],
                 'cat_id': item[23],
                 'active': active,
                 'identifier': list_of_identifiers,
            }
    return jsonify(response)

@app.route("/api/feedback", methods=['POST'])
def feedback():
    content = flask.request.json
    with psycopg2.connect(**config['atlas']) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS feedback (timestamp INT," 
                  "name VARCHAR(255), organisation VARCHAR(255), email VARCHAR(255), message VARCHAR(2047));")
        insert_sql = "INSERT INTO feedback (timestamp, name, organisation, email, message) VALUES (%s, %s, %s, %s, %s)"
        name = content['name']
        organisation = content['organisation']
        email = content['email']
        message = content['message']
        timestamp = int(time.time())
        try:
            c.execute(insert_sql, (timestamp, name, organisation, email, message))
        except Exception as error:
            return jsonify(data=content, status="fail", error=error.pgcode)
        finally:
            headers = {'Content-type': 'application/json', }
            sl_d = {
                "attachments": [
                    {
                        "fallback": "ATLAS feedback received",
                        "color": "#36a64f",
                        "author_name": name,
                        "author_link": "mailto:" + email,
                        "title": organisation,
                        "text": message,
                        "footer": "https://ccaf.tech/",
                        "footer_icon": "https://i.ibb.co/HPhL1xy/favicon.png",
                        "ts": timestamp
                    }
                ]
            }

            date_for_ms = datetime.utcfromtimestamp(timestamp).isoformat()
            ms_d = {
                "summary": "ATLAS feedback received",
                "themeColor": "FFB81C",
                "title": "ATLAS feedback received",
                "sections": [
                    {
                        "activityTitle": name,
                        "activitySubtitle": date_for_ms,
                        "activityImage": "https://i.ibb.co/0B6NSnK/user.jpg",
                        "facts": [
                            {
                                "name": "Organisation:",
                                "value": organisation
                            },
                            {
                                "name": "Email:",
                                "value": email
                            }
                        ],
                        "text": message
                    }
                ]
            }

            sl_d = str(sl_d)
            ms_d = str(ms_d)
            flask.g.slackmsg = (sl_d, headers)
            flask.g.msmsg = (ms_d, headers)

    return jsonify(data=content, status="success", error="")

@app.teardown_request
def teardown_request(_: Exception):
    if hasattr(flask.g, 'slackmsg'):
        try:
            sl_d, headers = flask.g.slackmsg
            requests.post(config['webhook'], headers=headers, data=sl_d)
        except Exception as error:
            app.logger.exception(str(error))
    if hasattr(flask.g, 'msmsg'):
        try:
            ms_d, headers = flask.g.msmsg
            requests.post(config['webhook_ms'], headers=headers, data=ms_d)
        except Exception as error:
            app.logger.exception(str(error))

@app.route("/api/suggest", methods=['POST'])
def suggest():
    content = flask.request.json
    with psycopg2.connect(**config['atlas']) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS suggest (timestamp INT," 
                  "id INT REFERENCES organisations(id), name VARCHAR(255), legalname VARCHAR(255),  "
                  "edate DATE, incdate DATE, leghq INT REFERENCES countries(id), leghqcity VARCHAR(123), "
                  "ophq INT REFERENCES countries(id), ophqcity VARCHAR(123), link VARCHAR(255), twitter VARCHAR(63), "
                  "x_segments VARCHAR(255), segments VARCHAR(511), "
                  "x_categories VARCHAR(1023), categories VARCHAR(2047), onbehalf BOOLEAN);")
        insert_sql = "INSERT INTO suggest (timestamp, id, name, legalname, edate, incdate, leghq, leghqcity, ophq, " \
                     "ophqcity, link, twitter, x_segments, segments, x_categories, categories, onbehalf) " \
                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        timestamp = int(time.time())
        if content['id'] == "None":
            org_id = None
        else:
            org_id = content['id']
        name = content['name']
        legalname = content['legalname']
        edate = content['edate']
        if content['incdate'] == "None":
            incdate = None
        else:
            incdate = content['incdate']
        if content['leghq'] == "None":
            leghq = None
        else:
            leghq = content['leghq']
        leghqcity = content['leghqcity']
        if content['ophq'] == "None":
            ophq = None
        else:
            ophq = content['ophq']
        ophqcity = content['ophqcity']
        link = content['link']
        twitter = content['twitter']
        x_segments = content['x_segments']
        segments = content['segments']
        x_categories = content['x_categories']
        categories = content['categories']
        onbehalf = content['onbehalf']

        try:
            c.execute(insert_sql, (timestamp, org_id, name, legalname, edate, incdate, leghq, leghqcity, ophq,
                                   ophqcity, link, twitter, x_segments, segments, x_categories, categories, onbehalf))
        except Exception as error:
            return jsonify(data=content, status="fail", error=error.pgcode)
        finally:
            headers = {'Content-type': 'application/json', }
            sugg = {
                "attachments": [
                    {
                        "fallback": "ATLAS company suggestion received",
                        "color": "#36a64f",
                        "author_name": 'New suggestion/edit: ' + name,
                        "author_link": 'http://' + link,
                        "title": segments,
                        "text": categories,
                        "footer": "https://ccaf.tech",
                        "footer_icon": "https://i.ibb.co/HPhL1xy/favicon.png",
                        "ts": timestamp
                    }
                ]
            }
            sugg = str(sugg)
            flask.g.slackmsg2 = (sugg, headers)

            date_for_ms = datetime.utcfromtimestamp(timestamp).isoformat()
            ms_sugg = {
                "summary": "ATLAS entity suggestion received",
                "themeColor": "FFB81C",
                "title": "ATLAS entity suggestion received",
                "sections": [
                    {
                        "activityTitle": name,
                        "activitySubtitle": date_for_ms,
                        "activityImage": "https://i.ibb.co/L0mWNPn/entity.png",
                        "facts": [
                            {
                                "name": "ID ('None' for new)",
                                "value": content['id']
                            },
                            {
                                "name": "Colloquial name:",
                                "value": name
                            },
                            {
                                "name": "Legal name:",
                                "value": legalname
                            },
                            {
                                "name": "Launch date:",
                                "value": edate
                            },
                            {
                                "name": "Incorporation date:",
                                "value": content['incdate']
                            },
                            {
                                "name": "Operational HQ (country)",
                                "value": content['ophq']
                            },
                            {
                                "name": "Operational HQ (city)",
                                "value": ophqcity
                            },
                            {
                                "name": "Legal HQ (country)",
                                "value": content['leghq']
                            },
                            {
                                "name": "Legal HQ (city)",
                                "value": leghqcity
                            },
                            {
                                "name": "Website:",
                                "value": link
                            },
                            {
                                "name": "Twitter:",
                                "value": twitter
                            },
                            {
                                "name": "Segment(s):",
                                "value": segments
                            },
                            {
                                "name": "Segment(s) code(s):",
                                "value": x_segments
                            },
                            {
                                "name": "Category(ies):",
                                "value": categories
                            },
                            {
                                "name": "Category(ies) code(s)",
                                "value": x_categories
                            },
                        ],
                    }
                ]
            }

            sugg = str(sugg)
            ms_sugg = str(ms_sugg)
            flask.g.slackmsg2 = (sugg, headers)
            flask.g.msmsg2 = (ms_sugg, headers)
            
    return jsonify(data=content, status="success", error="")

@app.teardown_request
def teardown_request2(_: Exception):
    if hasattr(flask.g, 'slackmsg2'):
        try:
            sugg, headers = flask.g.slackmsg2
            requests.post(config['webhook'], headers=headers, data=sugg)
        except Exception as error:
            app.logger.exception(str(error))
    if hasattr(flask.g, 'msmsg2'):
        try:
            ms_d, headers = flask.g.msmsg2
            requests.post(config['webhook_ms'], headers=headers, data=ms_d)
        except Exception as error:
            app.logger.exception(str(error))


# =============================================================================
# @app.route("/api/all", methods=['GET', 'POST'])
# def countries_btc():
#     response = []
#     for item in taxonomy:
#          response.append({
#             '!year': item[0],
#             'segm': item[1],
#             'desc': item[2],
#             'cat': item[4],
#             'subcat': item[6]
#             })
#     return jsonify(response)
# 
# =============================================================================

# =============================================================================
#             
# @app.route('/api/data/<value>')
# def recalculate_data(value):
#         
#     price = float(value)
# 
#     if price in cache:
#         return cache[price]
# 
#     k = 0.05/price 
#     # that is because base calculation in the DB is for the price 0.05 USD/KWth
#     # temporary vars:
#     prof_eqp = []
#     all_prof_eqp = []
#     max_all = []
#     min_all = []
#     ts_all = []
#     date_all = []
#     guess_all = []
#     response = []
#     
#     prof_th=pd.DataFrame(prof_threshold)
#     prof_th=prof_th.drop(1, axis=1).set_index(0)
#     prof_th_ma=prof_th.rolling(window=14, min_periods=1).mean()
#     
#     hashra=pd.DataFrame(hash_rate)
#     hashra=hashra.drop(1, axis=1).set_index(0)
#     
#     for timestamp, row in prof_th_ma.iterrows():
#         for miner in miners:
#             if timestamp>miner[1] and row[2]*k > miner[2]:
#                 prof_eqp.append(miner[2])
#             # ^^current date miner release date ^^checks if miner is profit. ^^adds miner's efficiency to the list
#         all_prof_eqp.append(prof_eqp)
#         try:
#             max_consumption = max(prof_eqp)*hashra[2][timestamp]*365.25*24/1e9*1.2
#             min_consumption = min(prof_eqp)*hashra[2][timestamp]*365.25*24/1e9*1.01
#             guess_consumption = sum(prof_eqp)/len(prof_eqp)*hashra[2][timestamp]*365.25*24/1e9*1.1
#         except:  # in case if mining is not profitable (it is impossible to find MIN or MAX of empty list)
#             max_consumption = max_all[-1]
#             min_consumption = min_all[-1]
#             guess_consumption = guess_all[-1]
#         max_all.append(max_consumption)
#         min_all.append(min_consumption)
#         guess_all.append(guess_consumption)
#         ts_all.append(timestamp)
#         date = datetime.utcfromtimestamp(timestamp).isoformat()
#         date_all.append(date)
#         prof_eqp = []
# 
#     energy_df = pd.DataFrame(list(zip(max_all, min_all, guess_all)), index=ts_all, columns=['MAX', 'MIN', 'GUESS'])
#     energy_ma = energy_df.rolling(window=7, min_periods=1).mean()
#     max_ma = list(energy_ma['MAX'])
#     min_ma = list(energy_ma['MIN'])
#     guess_ma = list(energy_ma['GUESS'])
#     
#     for day in range(0, len(ts_all)):
#         response.append({
#             'date': date_all[day],
#             'guess_consumption': round(guess_ma[day], 2),
#             'max_consumption': round(max_ma[day], 2),
#             'min_consumption': round(min_ma[day], 2),
#             'timestamp': ts_all[day],
#         })
# 
#     value = jsonify(data=response)
#     cache[price] = value
#     return value
# 
# 
# @app.route("/api/max/<value>")
# def recalculate_max(value):
#     price = float(value)
#     k = 0.05/price  # that is because base calculations in the DB is for the price 0.05 USD/KWth
#     prof_eqp = []   # temp var for the list of profitable equipment efficiency at any given moment
#     for miner in miners:
#         if prof_threshold[-1][0]>miner[1] and prof_threshold[-1][2]*k>miner[2]: prof_eqp.append(miner[2])
#         # ^^current date miner release date ^^checks if miner is profit. ^^if yes, adds miner's efficiency to the list
#     try:
#         max_consumption = max(prof_eqp)*hashrate*1.2/1e9
#     except:
#         max_consumption = 'mining is not profitable'
#     return jsonify(max_consumption)
# 
# 
# @app.route("/api/min/<value>")
# def recalculate_min(value):
#     price = float(value)
#     k = 0.05/price  # that is because base calculations in the DB is for the price 0.05 USD/KWth
#     prof_eqp = []  # temp var for the list of profitable equipment efficiency at any given moment
#     for miner in miners:
#         if prof_threshold[-1][0]>miner[1] and prof_threshold[-1][2]*k>miner[2]: prof_eqp.append(miner[2])
#         # ^^current date miner release date ^^checks if miner is profit. ^^if yes, adds miner's efficiency to the list
#     try:
#         min_consumption = min(prof_eqp)*hashrate*1.01/1e9
#     except:
#         min_consumption = 'mining is not profitable'
#     return jsonify(min_consumption)
# 
# 
# @app.route("/api/guess/<value>")
# def recalculate_guess(value):
#     price = float(value)
#     k = 0.05/price  # that is because base calculations in the DB is for the price 0.05 USD/KWth
#     prof_eqp = []   # temp var for the list of profitable equipment efficiency at any given moment
# 
#     for miner in miners:
#         if prof_threshold[-1][0]>miner[1] and prof_threshold[-1][2]*k>miner[2]: prof_eqp.append(miner[2])
#         # ^^current date miner release date ^^checks if miner is profit. ^^if yes, adds miner's efficiency to the list
#     try:
#         guess_consumption = sum(prof_eqp)/len(prof_eqp)*hashrate*1.10/1e9
#     except:
#         guess_consumption = 'mining is not profitable'
#     return jsonify(guess_consumption)
# # =============================================================================
# # @app.route("/api/countries", methods=['GET','POST'])
# # def countries_old():
# #     jsonify(data=countries)
# # =============================================================================
# 
# 
# @app.route("/api/countries", methods=['GET', 'POST'])
# def countries_btc():
#     tup2dict = {a:[c,d] for a,b,c,d in countries}
#     tup2dict['Bitcoin'][0] = round(cons[-1][4],2)
#     dictsort = sorted(tup2dict.items(), key = lambda i: i[1][0], reverse=True)
#     response = []
#     for item in dictsort:
#          response.append({
#             'country': item[0],
#             'y': item[1][0],        
#             'x': dictsort.index(item)+1,
#             'bitcoin_percentage': round(item[1][0]/round(cons[-1][4], 2)*100, 2),
#             'logo': item[1][1]
#             })
#     for item in response:
#         if item['country'] == "Bitcoin":
#             item['color'] = "#ffb81c"
#     return jsonify(response)
# 
# 
# @app.route("/api/feedback", methods=['POST'])
# def feedback():
#     content = flask.request.json
#     with psycopg2.connect(**config['custom_data']) as conn:
#         c = conn.cursor()
#         c.execute("CREATE TABLE IF NOT EXISTS feedback (timestamp INT PRIMARY KEY," 
#                   "name TEXT, organisation TEXT, email TEXT, message TEXT);")
#         insert_sql = "INSERT INTO feedback (timestamp, name, organisation, email, message) VALUES (%s, %s, %s, %s, %s)"
#         name = content['name']
#         organisation = content['organisation']
#         email = content['email']
#         message = content['message']
#         timestamp = int(time.time())
#         try:
#             c.execute(insert_sql, (timestamp, name, organisation, email, message))
#         except Exception as error:
#             return jsonify(data=content, status="fail", error=error.pgcode)
#         finally:
#             headers = {'Content-type': 'application/json', }
#             sl_d = {
#                 "attachments": [
#                     {
#                         "fallback": "CBECI feedback recieved",
#                         "color": "#36a64f",
#                         "author_name": name,
#                         "author_link": "mailto:" + email,
#                         "title": organisation,
#                         "text": message,
#                         "footer": "cbeci.org",
#                         "footer_icon": "https://i.ibb.co/HPhL1xy/favicon.png",
#                         "ts": timestamp
#                     }
#                 ]
#             }
#             sl_d = str(sl_d)
#             flask.g.slackmsg = (sl_d, headers)
#     return jsonify(data=content, status="success", error="")
# 
# 
# @app.teardown_request
# def teardown_request(_: Exception):
#     if hasattr(flask.g, 'slackmsg'):
#         try:
#             sl_d, headers = flask.g.slackmsg
#             requests.post(config['webhook'], headers=headers, data=sl_d)
#         except Exception as error:
#             app.logger.exception(str(error))
# =============================================================================


# =============================================================================
# # ====== test endpoints ahead =========================================
# @app.route('/api/new/data/<value>')
# def recalculate_data_new(value):
#     price = float(value)
# 
# #    if price in cache:
# #        return cache[price]
# 
#     k = 0.05 / price
#     # that is because base calculation in the DB is for the price 0.05 USD/KWth
#     # temporary vars:
#     prof_eqp = []
#     all_prof_eqp = []
#     max_all = []
#     min_all = []
#     ts_all = []
#     date_all = []
#     guess_all = []
#     response = []
# 
#     for i in range(0, len(prof_threshold)):
#         for miner in miners:
#             if prof_threshold[i][0] > miner[1] and prof_threshold[i][2] * k > miner[2]:
#                 prof_eqp.append(miner[2])
#             # ^^current date miner release date ^^checks if miner is profit. ^^adds miner's efficiency to the list
#         all_prof_eqp.append(prof_eqp)
#         try:
#             max_consumption = max(prof_eqp) * hash_rate[i][2] * 365.25 * 24 / 1e9 * 1.2
#             min_consumption = min(prof_eqp) * hash_rate[i][2] * 365.25 * 24 / 1e9 * 1.01
#             guess_consumption = sum(prof_eqp) / len(prof_eqp) * hash_rate[i][2] * 365.25 * 24 / 1e9 * 1.1
#         except:  # in case if mining is not profitable (it is impossible to find MIN or MAX of empty list)
#             max_consumption = max_all[-1]
#             min_consumption = min_all[-1]
#             guess_consumption = guess_all[-1]
#         max_all.append(max_consumption)
#         min_all.append(min_consumption)
#         guess_all.append(guess_consumption)
#         timestamp = prof_threshold[i][0]
#         ts_all.append(timestamp)
#         date = prof_threshold[i][1]
#         date_all.append(date)
#         prof_eqp = []
# 
#     energy_df = pd.DataFrame(list(zip(max_all, min_all, guess_all)), index=ts_all, columns=['MAX', 'MIN', 'GUESS'])
#     energy_ma = energy_df.rolling(window=7, min_periods=1).mean()
#     max_ma = list(energy_ma['MAX'])
#     min_ma = list(energy_ma['MIN'])
#     guess_ma = list(energy_ma['GUESS'])
# 
#     for day in range(0, len(ts_all)):
#         response.append({
#             'g': round(guess_ma[day], 2),
#             'x': round(max_ma[day], 2),
#             'n': round(min_ma[day], 2),
#             't': ts_all[day],
#         })
# 
# #    value = jsonify(data=response)
# #    cache[price] = value
# #    return value
#     return jsonify(data=response)
# 
# 
# @app.route("/api/new/countries", methods=['GET', 'POST'])
# def countries_btc_new():
#     tup2dict = {a: [c, d, b] for a, b, c, d in countries}
#     tup2dict['Bitcoin'][0] = round(cons[-1][4],2)
#     dictsort = sorted(tup2dict.items(), key=lambda i: i[1][0], reverse=True)
#     response = []
#     for item in dictsort:
#          response.append({
#             'c': item[0],
#             'y': item[1][0],
#             'x': dictsort.index(item)+1,
#             'p': round(item[1][0]/round(cons[-1][4], 2)*100, 2),
#             'l': item[1][2]
#             })
#     for item in response:
#         if item['c'] == "Bitcoin":
#             item['color'] = "#ffb81c"
#     return jsonify(response)
# 
# =============================================================================

if __name__ == '__main__':
    app.run(host='0.0.0.0', use_reloader=True)
