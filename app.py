import os
import platform
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psy
from dotenv import dotenv_values
import json
from flask import Response


@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')

# Zadanie 1
@app.route('/v1/health', methods=['GET'])
def do_stuff():
    conn = connect()

    cur = conn.cursor()
    cur.execute("SELECT VERSION()")
    fetched_version = cur.fetchone()

    cur.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size")
    fetched_size = cur.fetchone()



    dic = {}
    dic2 = {}

    dic2['pgsql'] = dic
    dic["dota2_db_size"] = fetched_size[0]
    dic['version'] = fetched_version[0]

    json_string = json.dumps(dic2)

    return json_string

# Zadanie 2
# 2
@app.route('/v2/players/<string:player_id>/game_exp/', methods=['GET'])
def get_game_exp():
    pass

# 1
@app.route('/v2/patches', methods=['GET'])
def get_patches():
    conn = connect()

    cur = conn.cursor()
    cur.execute("select vsetkypece.patch_version, vsetkypece.patch_start_date, vsetkypece.patch_end_date, matches.id as match_id, duration "
                "from matches "
                "left join ("
                "select patches.name as patch_version, "
                "extract(epoch from patches.release_date) as patch_start_date, "
                "extract (epoch from next_patch.release_date) as patch_end_date "
                "from patches "
                "left join patches as next_patch "
                "on patches.id = next_patch.id  - 1 "
                "order by patches.id"
                ") as vsetkypece on (matches.start_time > vsetkypece.patch_start_date "
                "and matches.start_time < coalesce (vsetkypece.patch_end_date, 9999999999))"
                )

    dic = {}
    dic['patches'] = []

    for column in cur:
        current_patch = None


        for patch in dic['patches']:
            # column patch_version
            if patch['patch_version'] == str(column[0]):
                current_patch = patch
                break

        if current_patch is not None:
            match = {}

            match['match_id'] = column[3]
            match['duration'] = column[4]

            current_patch['matches'].append(match)


        else:
            current_patch = {}
            current_patch['patch_version'] = column[0]
            current_patch['patch_start_date'] = column[1]
            current_patch['patch_end_date'] = column[2]

            current_patch['matches'] = []
            match = {}

            match['match_id'] = column[3]
            match['duration'] = column[4]

            current_patch['matches'].append(match)
            dic['patches'].append(current_patch)

    json_string = json.dumps(dic)

    return json_string



def linux_version():
    var = dotenv_values("/home/en_var.env")

    return psy.connect(
        host="147.175.150.216",
        database="dota2",
        user=var['DBUSER'],
        password=var['DBPASS'])

def win_version():

    return psy.connect(
        host="147.175.150.216",
        database="dota2",
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'))

def connect():
    if platform.system() == "Linux":
        return linux_version()
    else:
        return win_version()




@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()