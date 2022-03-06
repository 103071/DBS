from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psy
from dotenv import dotenv_values
import json



@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')


@app.route('/v1/health', methods=['GET'])
def do_stuff():
    print('Request for index page received')
    var = dotenv_values("/home/en_var.env")

    conn = psy.connect(
       host="147.175.150.216",
       database="dota2",
       user=var['DBUSER'],
       password=var['DBPASS'])

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