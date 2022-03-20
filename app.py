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
    cur.close()

    return json_string

# Zadanie 2
# 1
@app.route('/v2/patches/', methods=['GET'])
def get_patches():
    conn = connect()

    cur = conn.cursor()
    cur.execute("select vsetkymece.match_id, round (vsetkymece.duration/60.0, 2) as duration, patches.name as patch_version, "
                "cast (extract(epoch from patches.release_date) as INT) as patch_start_date, "
                "cast (extract (epoch from next_patch.release_date) as INT) as patch_end_date "
                "from patches "
                "left join patches as next_patch on patches.id = next_patch.id  - 1 "
                "left join ("
                "select matches.id as match_id, duration, start_time "
                "from matches "
                ") as vsetkymece on (vsetkymece.start_time > extract(epoch from patches.release_date) "
                "and vsetkymece.start_time < coalesce (extract (epoch from next_patch.release_date) , 9999999999)) "
                "order by patches.id"
                )

    dic = {}
    dic['patches'] = []

    for column in cur:
        current_patch = None


        for patch in dic['patches']:
            # column patch_version
            if patch['patch_version'] == str(column[2]):
                current_patch = patch
                break

        if current_patch is not None:
            match = {}

            match['match_id'] = column[0]
            match['duration'] = float(column[1])

            current_patch['matches'].append(match)


        else:
            current_patch = {}
            current_patch['patch_version'] = column[2]
            current_patch['patch_start_date'] = column[3]
            current_patch['patch_end_date'] = column[4]

            current_patch['matches'] = []

            if column[0] is not None:
                match = {}

                match['match_id'] = column[0]
                match['duration'] = float(column[1])

                current_patch['matches'].append(match)

            dic['patches'].append(current_patch)

    json_string = json.dumps(dic)
    cur.close()

    return json_string



# 2
@app.route('/v2/players/<string:player_id>/game_exp/', methods=['GET'])
def get_game_exp(player_id):
    conn = connect()

    cur = conn.cursor()
    cur.execute("select coalesce (nick, 'nick') from players "
                "where id = " + player_id)

    matches = []
    dic = {}


    dic['id'] = int(player_id)
    dic['player_nick'] = cur.fetchone()[0]

    cur.execute("select pl.id, coalesce (pl.nick,'unknown') as player_nick, match_id, "
        "localized_name as hero_localized_name, "
        "round (m.duration / 60.0, 2) as match_duration_minutes, "
        "coalesce (xp_hero, 0) + coalesce(xp_creep,0) + coalesce(xp_other,0) + coalesce(xp_roshan,0) as experiences_gained, "
        "greatest(level) as level_gained, "
        "case when mpd.player_slot < 5 and m.radiant_win = true or mpd.player_slot > 127 and m.radiant_win = false "
        "then true else false end as winner "
        "from matches_players_details as mpd "
        "join players as pl "
        "on pl.id = mpd.player_id "
        "join heroes as hero "
        "on mpd.hero_id = hero.id "
        "join matches as m "
        "on mpd.match_id = m.id "
        "where pl.id = " + player_id +
        " order by m.id"
    )


    for column in cur:
        match = {}
        match['match_id'] = column[2]
        match['hero_localized_name'] = column[3]
        match['match_duration_minutes'] = float(column[4])
        match['experiences_gained'] = column[5]
        match['level_gained'] = column[6]
        match['winner'] = column[7]

        matches.append(match)

    dic['matches'] = matches

    json_string = json.dumps(dic)
    cur.close()

    return json_string

# 3
@app.route('/v2/players/<string:player_id>/game_objectives/', methods=['GET'])
def game_objectives(player_id):
    conn = connect()

    cur = conn.cursor()
    cur.execute("select pl.id, pl.nick as player_nick, mpd.match_id, heroes.localized_name, "
                "coalesce(game_objectives.subtype, 'NO ACTION') "
                "from players as pl "
                "left join matches_players_details as mpd on mpd.player_id = pl.id "
                "left join heroes on heroes.id = mpd.hero_id "
                "left join game_objectives on game_objectives.match_player_detail_id_1 = mpd.id "
                "where pl.id = " + player_id +
                " order by mpd.match_id")

    dic = {}
    dic['id'] = player_id
    matches = []

    dic['matches'] = matches

    for column in cur:
        if not dic.contains('player_nick'):
            dic['player_nick'] = column[1]

        current_match = None
        for match in matches:
            if match['match_id'] == column[2]:
                current_match = match['match_id']

    json_string = json.dumps(dic)
    cur.close()

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