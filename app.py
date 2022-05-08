import os
import platform
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
import psycopg2 as psy
from dotenv import dotenv_values
import json
from flask import Response

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, DateTime, ForeignKey, Index, Integer, Numeric, \
    SmallInteger, String, Table, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

app = Flask(__name__)
Base = declarative_base()
metadata = Base.metadata


alchemy_env = dotenv_values("/home/en_var.env")
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + alchemy_env['DBUSER'] + ':' + alchemy_env['DBPASS'] + '@147.175.150.216/dota2'
#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + os.getenv('DBUSER') + ':' + os.getenv('DBPASS') + '@147.175.150.216/dota2'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)



class Ability(db.Model):
    __tablename__ = 'abilities'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class AuthGroup(db.Model):
    __tablename__ = 'auth_group'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('auth_group_id_seq'::regclass)"))
    name = db.Column(db.String(150), nullable=False, unique=True)


class AuthUser(db.Model):
    __tablename__ = 'auth_user'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('auth_user_id_seq'::regclass)"))
    password = db.Column(db.String(128), nullable=False)
    last_login = db.Column(db.DateTime(True))
    is_superuser = db.Column(db.Boolean, nullable=False)
    username = db.Column(db.String(150), nullable=False, unique=True)
    first_name = db.Column(db.String(150), nullable=False)
    last_name = db.Column(db.String(150), nullable=False)
    email = db.Column(db.String(254), nullable=False)
    is_staff = db.Column(db.Boolean, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False)
    date_joined = db.Column(db.DateTime(True), nullable=False)


class ClusterRegion(db.Model):
    __tablename__ = 'cluster_regions'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class DjangoContentType(db.Model):
    __tablename__ = 'django_content_type'
    __table_args__ = (
        UniqueConstraint('app_label', 'model'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('django_content_type_id_seq'::regclass)"))
    app_label = db.Column(db.String(100), nullable=False)
    model = db.Column(db.String(100), nullable=False)


class DjangoMigration(db.Model):
    __tablename__ = 'django_migrations'

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('django_migrations_id_seq'::regclass)"))
    app = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    applied = db.Column(db.DateTime(True), nullable=False)


class DjangoSession(db.Model):
    __tablename__ = 'django_session'

    session_key = db.Column(db.String(40), primary_key=True, index=True)
    session_data = db.Column(db.Text, nullable=False)
    expire_date = db.Column(db.DateTime(True), nullable=False, index=True)


class DoctrineMigrationVersion(db.Model):
    __tablename__ = 'doctrine_migration_versions'

    version = db.Column(db.String(191), primary_key=True)
    executed_at = db.Column(db.TIMESTAMP(), server_default=text("NULL::timestamp without time zone"))
    execution_time = db.Column(db.Integer)


class FlywaySchemaHistory(db.Model):
    __tablename__ = 'flyway_schema_history'

    installed_rank = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.String(50))
    description = db.Column(db.String(200), nullable=False)
    type = db.Column(db.String(20), nullable=False)
    script = db.Column(db.String(1000), nullable=False)
    checksum = db.Column(db.Integer)
    installed_by = db.Column(db.String(100), nullable=False)
    installed_on = db.Column(db.DateTime, nullable=False, server_default=text("now()"))
    execution_time = db.Column(db.Integer, nullable=False)
    success = db.Column(db.Boolean, nullable=False, index=True)


class Hero(db.Model):
    __tablename__ = 'heroes'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    localized_name = db.Column(db.Text)


class Item(db.Model):
    __tablename__ = 'items'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class Migration(db.Model):
    __tablename__ = 'migrations'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('migrations_id_seq'::regclass)"))
    migration = db.Column(db.String(255), nullable=False)
    batch = db.Column(db.Integer, nullable=False)


class Patch(db.Model):
    __tablename__ = 'patches'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('patches_id_seq'::regclass)"))
    name = db.Column(db.Text, nullable=False)
    release_date = db.Column(db.DateTime, nullable=False)


class Player(db.Model):
    __tablename__ = 'players'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    nick = db.Column(db.Text)


t_propel_migration = Table(
    'propel_migration', metadata,
    db.Column('version', db.Integer, server_default=text("0"))
)


class AuthPermission(db.Model):
    __tablename__ = 'auth_permission'
    __table_args__ = (
        UniqueConstraint('content_type_id', 'codename'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('auth_permission_id_seq'::regclass)"))
    name = db.Column(db.String(255), nullable=False)
    content_type_id = db.Column(db.ForeignKey('django_content_type.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    codename = db.Column(db.String(100), nullable=False)

    content_type = db.relationship('DjangoContentType')


class AuthUserGroup(db.Model):
    __tablename__ = 'auth_user_groups'
    __table_args__ = (
        UniqueConstraint('user_id', 'group_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('auth_user_groups_id_seq'::regclass)"))
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    group_id = db.Column(db.ForeignKey('auth_group.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = db.relationship('AuthGroup')
    user = db.relationship('AuthUser')


class DjangoAdminLog(db.Model):
    __tablename__ = 'django_admin_log'
    __table_args__ = (
        CheckConstraint('action_flag >= 0'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('django_admin_log_id_seq'::regclass)"))
    action_time = db.Column(db.DateTime(True), nullable=False)
    object_id = db.Column(db.Text)
    object_repr = db.Column(db.String(200), nullable=False)
    action_flag = db.Column(db.SmallInteger, nullable=False)
    change_message = db.Column(db.Text, nullable=False)
    content_type_id = db.Column(db.ForeignKey('django_content_type.id', deferrable=True, initially='DEFERRED'), index=True)
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    content_type = db.relationship('DjangoContentType')
    user = db.relationship('AuthUser')


class Match(db.Model):
    __tablename__ = 'matches'

    id = db.Column(db.Integer, primary_key=True)
    cluster_region_id = db.Column(db.ForeignKey('cluster_regions.id'))
    start_time = db.Column(db.Integer)
    duration = db.Column(db.Integer)
    tower_status_radiant = db.Column(db.Integer)
    tower_status_dire = db.Column(db.Integer)
    barracks_status_radiant = db.Column(db.Integer)
    barracks_status_dire = db.Column(db.Integer)
    first_blood_time = db.Column(db.Integer)
    game_mode = db.Column(db.Integer)
    radiant_win = db.Column(db.Boolean)
    negative_votes = db.Column(db.Integer)
    positive_votes = db.Column(db.Integer)

    cluster_region = db.relationship('ClusterRegion')


class PlayerRating(db.Model):
    __tablename__ = 'player_ratings'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('player_ratings_id_seq'::regclass)"))
    player_id = db.Column(db.ForeignKey('players.id'))
    total_wins = db.Column(db.Integer)
    total_matches = db.Column(db.Integer)
    trueskill_mu = db.Column(db.Numeric)
    trueskill_sigma = db.Column(db.Numeric)

    player = db.relationship('Player')


class AuthGroupPermission(db.Model):
    __tablename__ = 'auth_group_permissions'
    __table_args__ = (
        UniqueConstraint('group_id', 'permission_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('auth_group_permissions_id_seq'::regclass)"))
    group_id = db.Column(db.ForeignKey('auth_group.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    permission_id = db.Column(db.ForeignKey('auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = db.relationship('AuthGroup')
    permission = db.relationship('AuthPermission')


class AuthUserUserPermission(db.Model):
    __tablename__ = 'auth_user_user_permissions'
    __table_args__ = (
        UniqueConstraint('user_id', 'permission_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('auth_user_user_permissions_id_seq'::regclass)"))
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    permission_id = db.Column(db.ForeignKey('auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    permission = db.relationship('AuthPermission')
    user = db.relationship('AuthUser')


class MatchesPlayersDetail(db.Model):
    __tablename__ = 'matches_players_details'
    __table_args__ = (
        Index('idx_match_id_player_id', 'match_id', 'player_slot', 'id'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('matches_players_details_id_seq'::regclass)"))
    match_id = db.Column(db.ForeignKey('matches.id'))
    player_id = db.Column(db.ForeignKey('players.id'))
    hero_id = db.Column(db.ForeignKey('heroes.id'))
    player_slot = db.Column(db.Integer)
    gold = db.Column(db.Integer)
    gold_spent = db.Column(db.Integer)
    gold_per_min = db.Column(db.Integer)
    xp_per_min = db.Column(db.Integer)
    kills = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    assists = db.Column(db.Integer)
    denies = db.Column(db.Integer)
    last_hits = db.Column(db.Integer)
    stuns = db.Column(db.Integer)
    hero_damage = db.Column(db.Integer)
    hero_healing = db.Column(db.Integer)
    tower_damage = db.Column(db.Integer)
    item_id_1 = db.Column(db.ForeignKey('items.id'))
    item_id_2 = db.Column(db.ForeignKey('items.id'))
    item_id_3 = db.Column(db.ForeignKey('items.id'))
    item_id_4 = db.Column(db.ForeignKey('items.id'))
    item_id_5 = db.Column(db.ForeignKey('items.id'))
    item_id_6 = db.Column(db.ForeignKey('items.id'))
    level = db.Column(db.Integer)
    leaver_status = db.Column(db.Integer)
    xp_hero = db.Column(db.Integer)
    xp_creep = db.Column(db.Integer)
    xp_roshan = db.Column(db.Integer)
    xp_other = db.Column(db.Integer)
    gold_other = db.Column(db.Integer)
    gold_death = db.Column(db.Integer)
    gold_abandon = db.Column(db.Integer)
    gold_sell = db.Column(db.Integer)
    gold_destroying_structure = db.Column(db.Integer)
    gold_killing_heroes = db.Column(db.Integer)
    gold_killing_creeps = db.Column(db.Integer)
    gold_killing_roshan = db.Column(db.Integer)
    gold_killing_couriers = db.Column(db.Integer)

    hero = db.relationship('Hero')
    item = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_1 == Item.id')
    item1 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_2 == Item.id')
    item2 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_3 == Item.id')
    item3 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_4 == Item.id')
    item4 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_5 == Item.id')
    item5 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_6 == Item.id')
    match = db.relationship('Match')
    player = db.relationship('Player')


class Teamfight(db.Model):
    __tablename__ = 'teamfights'
    __table_args__ = (
        Index('teamfights_match_id_start_teamfight_id_idx', 'match_id', 'start_teamfight', 'id'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('teamfights_id_seq'::regclass)"))
    match_id = db.Column(db.ForeignKey('matches.id'))
    start_teamfight = db.Column(db.Integer)
    end_teamfight = db.Column(db.Integer)
    last_death = db.Column(db.Integer)
    deaths = db.Column(db.Integer)

    match = db.relationship('Match')


class AbilityUpgrade(db.Model):
    __tablename__ = 'ability_upgrades'

    id = db.Column(Integer, primary_key=True, server_default=text("nextval('ability_upgrades_id_seq'::regclass)"))
    ability_id = db.Column(db.ForeignKey('abilities.id'))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    level = db.Column(Integer)
    time = db.Column(Integer)

    ability = db.relationship('Ability')
    match_player_detail = db.relationship('MatchesPlayersDetail')


class Chat(db.Model):
    __tablename__ = 'chats'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('chats_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    message = db.Column(db.Text)
    time = db.Column(db.Integer)
    nick = db.Column(db.Text)

    match_player_detail = relationship('MatchesPlayersDetail')


class GameObjective(db.Model):
    __tablename__ = 'game_objectives'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('game_objectives_id_seq'::regclass)"))
    match_player_detail_id_1 = db.Column(db.ForeignKey('matches_players_details.id'))
    match_player_detail_id_2 = db.Column(db.ForeignKey('matches_players_details.id'))
    key = db.Column(db.Integer)
    subtype = db.Column(db.Text)
    team = db.Column(db.Integer)
    time = db.Column(db.Integer)
    value = db.Column(db.Integer)
    slot = db.Column(db.Integer)

    matches_players_detail = db.relationship('MatchesPlayersDetail', primaryjoin='GameObjective.match_player_detail_id_1 == MatchesPlayersDetail.id')
    matches_players_detail1 = db.relationship('MatchesPlayersDetail', primaryjoin='GameObjective.match_player_detail_id_2 == MatchesPlayersDetail.id')


class PlayerAction(db.Model):
    __tablename__ = 'player_actions'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('player_actions_id_seq'::regclass)"))
    unit_order_none = db.Column(db.Integer)
    unit_order_move_to_position = db.Column(db.Integer)
    unit_order_move_to_target = db.Column(db.Integer)
    unit_order_attack_move = db.Column(db.Integer)
    unit_order_attack_target = db.Column(db.Integer)
    unit_order_cast_position = db.Column(db.Integer)
    unit_order_cast_target = db.Column(db.Integer)
    unit_order_cast_target_tree = db.Column(db.Integer)
    unit_order_cast_no_target = db.Column(db.Integer)
    unit_order_cast_toggle = db.Column(db.Integer)
    unit_order_hold_position = db.Column(db.Integer)
    unit_order_train_ability = db.Column(db.Integer)
    unit_order_drop_item = db.Column(db.Integer)
    unit_order_give_item = db.Column(db.Integer)
    unit_order_pickup_item = db.Column(db.Integer)
    unit_order_pickup_rune = db.Column(db.Integer)
    unit_order_purchase_item = db.Column(db.Integer)
    unit_order_sell_item = db.Column(db.Integer)
    unit_order_disassemble_item = db.Column(db.Integer)
    unit_order_move_item = db.Column(db.Integer)
    unit_order_cast_toggle_auto = db.Column(db.Integer)
    unit_order_stop = db.Column(db.Integer)
    unit_order_buyback = db.Column(db.Integer)
    unit_order_glyph = db.Column(db.Integer)
    unit_order_eject_item_from_stash = db.Column(db.Integer)
    unit_order_cast_rune = db.Column(db.Integer)
    unit_order_ping_ability = db.Column(db.Integer)
    unit_order_move_to_direction = db.Column(db.Integer)
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))

    match_player_detail = db.relationship('MatchesPlayersDetail')


class PlayerTime(db.Model):
    __tablename__ = 'player_times'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('player_times_id_seq'::regclass)"))
    match_player_detail_id = db.Column(ForeignKey('matches_players_details.id'))
    time = db.Column(db.Integer)
    gold = db.Column(db.Integer)
    lh = db.Column(db.Integer)
    xp = db.Column(db.Integer)

    match_player_detail = db.relationship('MatchesPlayersDetail')


class PurchaseLog(db.Model):
    __tablename__ = 'purchase_logs'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('purchase_logs_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    item_id = db.Column(db.ForeignKey('items.id'))
    time = db.Column(db.Integer)

    item = db.relationship('Item')
    match_player_detail = db.relationship('MatchesPlayersDetail')


class TeamfightsPlayer(db.Model):
    __tablename__ = 'teamfights_players'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('teamfights_players_id_seq'::regclass)"))
    teamfight_id = db.Column(db.ForeignKey('teamfights.id'))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    buyback = db.Column(db.Integer)
    damage = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    gold_delta = db.Column(db.Integer)
    xp_start = db.Column(db.Integer)
    xp_end = db.Column(db.Integer)

    match_player_detail = db.relationship('MatchesPlayersDetail')
    teamfight = db.relationship('Teamfight')


@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')

# Zadanie 2
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


# ZADANIE 4
@app.route('/v4/players/<string:player_id>/game_exp/', methods=['GET'])
def orm_game_exp(player_id):
    player = Player.query.join(MatchesPlayersDetail, MatchesPlayersDetail.player_id == Player.id)\
            .join(Hero, MatchesPlayersDetail.hero_id == Hero.id) \
            .join(Match, MatchesPlayersDetail.match_id == Match.id) \
            .filter(Player.id == player_id) \
        .with_entities(
            (db.func.round(db.cast(Match.duration, db.Numeric) / 60, 2)).label("match_duration_minutes"),
            (db.func.greatest(MatchesPlayersDetail.level)).label('level_gained'),
            db.case([
                db.and_(MatchesPlayersDetail.player_slot < 5, Match.radiant_win),
                db.and_(db.not_(Match.radiant_win), MatchesPlayersDetail.player_slot > 127)
            ]).label("winner"),

            (db.func.coalesce(MatchesPlayersDetail.xp_hero, 0) + db.func.coalesce(MatchesPlayersDetail.xp_creep, 0) +
            db.func.coalesce(MatchesPlayersDetail.xp_other, 0) + db.func.coalesce(MatchesPlayersDetail.xp_roshan, 0)).label("experiences_gained")) \
        .add_columns(
            Player.id,
            db.func.coalesce(Player.nick, 'unknown').label('player_nick'),
            Hero.localized_name.label('hero_localized_name'),
            Match.id.label('match_id'),
        ) \
        .order_by(Match.id).all()

    dic = {}
    dic["id"] = int(player_id)
    dic["player_nick"] = player[0].player_nick
    matches = []

    for p in player:
        match = {}
        match['match_id'] = p.match_id
        match['hero_localized_name'] = p.hero_localized_name
        match['match_duration_minutes'] = float(p.match_duration_minutes)
        match['experiences_gained'] = p.experiences_gained
        match['level_gained'] = p.level_gained
        match['winner'] = p.winner

        matches.append(match)

    dic['matches'] = matches

    return Response(json.dumps(dic), status=200, mimetype="application/json")




@app.route('/v4/players/<string:player_id>/game_objectives/', methods=['GET'])
def orm_game_objectives(player_id):
    player = Player.query.join(MatchesPlayersDetail, MatchesPlayersDetail.player_id == Player.id) \
            .join(Hero, MatchesPlayersDetail.hero_id == Hero.id) \
            .join(GameObjective, GameObjective.match_player_detail_id_1 == MatchesPlayersDetail.id) \
            .filter(Player.id == player_id) \
            .with_entities (
                db.func.coalesce(GameObjective.subtype, 'NO_ACTION').label("subtype"),
            ) \
            .add_columns(
                Player.id,
                Player.nick.label("player_nick"),
                MatchesPlayersDetail.match_id,
                Hero.localized_name.label("hero_localized_name")
            ) \
            .order_by(MatchesPlayersDetail.match_id, GameObjective.subtype).all()

    dic = {}
    dic['id'] = int(player_id)
    matches = []

    for p in player:
        if not 'player_nick' in dic.keys():
            dic['player_nick'] = p.player_nick

        current_match = None

        for match in matches:
            if match['match_id'] == p.match_id:
                current_match = match
                break

        if current_match is not None:
            current = None

            for action in current_match['actions']:
                if action['hero_action'] == p.subtype:
                    current = action
                    break

            if current is not None:
                current['count'] += 1

            else:
                current = {}
                current['hero_action'] = p.subtype
                current['count'] = 1
                current_match['actions'].append(current)


        else:
            current_match = {}
            current_match['match_id'] = p.match_id
            current_match['hero_localized_name'] = p.hero_localized_name
            matches.append(current_match)

            current_match['actions'] = []
            action = {}
            action['hero_action'] = p.subtype
            action['count'] = 1
            current_match['actions'].append(action)

    dic['matches'] = matches


    return Response(json.dumps(dic), status=200, mimetype="application/json")


# Zadanie 3
# 1
@app.route('/v2/patches/', methods=['GET'])
def get_patches():
    conn = connect()

    cur = conn.cursor()
    cur.execute("select vsetkymece.match_id, round (vsetkymece.duration/60.0, 2) as duration, patches.name as patch_version, "
                "cast (extract (epoch from patches.release_date) as INT) as patch_start_date, "
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
                "coalesce(game_objectives.subtype, 'NO_ACTION') "
                "from players as pl "
                "left join matches_players_details as mpd on mpd.player_id = pl.id "
                "left join heroes on heroes.id = mpd.hero_id "
                "left join game_objectives on game_objectives.match_player_detail_id_1 = mpd.id "
                "where pl.id = " + player_id +
                " order by mpd.match_id, subtype")

    dic = {}
    matches = []

    dic['id'] = int(player_id)


    for column in cur:
        if not 'player_nick' in dic.keys():
            dic['player_nick'] = column[1]

        current_match = None

        for match in matches:
            if match['match_id'] == column[2]:
                current_match = match
                break

        if current_match is not None:
            current = None

            for action in current_match['actions']:
                if action['hero_action'] == column[4]:
                    current = action
                    break

            if current is not None:
                current['count'] += 1

            else:
                current = {}
                current['hero_action'] = column[4]
                current['count'] = 1
                current_match['actions'].append(current)


        else:
            current_match = {}
            current_match['match_id'] = column[2]
            current_match['hero_localized_name'] = column[3]
            matches.append(current_match)

            current_match['actions'] = []
            action = {}
            action['hero_action'] = column[4]
            action['count'] = 1
            current_match['actions'].append(action)

    dic['matches'] = matches

    json_string = json.dumps(dic)
    cur.close()

    return json_string




# ZADANIE 5
# 1
@app.route('/v3/matches/<string:match_id>/top_purchases/', methods=['GET'])
def top_purch(match_id):
    conn = connect()

    cur = conn.cursor()
    cur.execute(("WITH query_d as ( "
                 "SELECT items.name, mt.radiant_win, mt_p_det.id mpd_id, mt.id match_id, "
                 "heroes.localized_name, mt_p_det.player_id, mt_p_det.hero_id, "
                 "mt_p_det.player_slot, p.item_id, "
                 "COUNT (p.item_id) buy_count, "
                 "ROW_NUMBER() OVER (PARTITION BY mt_p_det.id ORDER BY COUNT(p.item_id) DESC, items.name ASC) "
                 "FROM matches mt "
                 "JOIN matches_players_details mt_p_det ON mt_p_det.match_id = mt.id "
                 "JOIN purchase_logs p ON mt_p_det.id = match_player_detail_id "
                 "JOIN heroes ON mt_p_det.hero_id = heroes.id "
                 "JOIN items ON p.item_id = items.id "
                 "WHERE mt.id = {} AND CASE "
                 "WHEN mt_p_det.player_slot >= 0 AND mt_p_det.player_slot <= 4 AND mt.radiant_win = true THEN true "
                 "WHEN mt_p_det.player_slot >= 128 AND mt_p_det.player_slot <= 132 AND mt.radiant_win = false THEN true "
                 "END "
                 "GROUP BY(mt.id,heroes.localized_name, items.name, mpd_id, p.item_id) "
                 "ORDER BY mt_p_det.hero_id ASC, buy_count DESC, items.name ASC) "
                 "SELECT * FROM query_d "
                 "WHERE ROW_NUMBER <= 5 ").format(match_id))


    dic = {}
    dic['id'] = int(match_id)
    heroes = []

    for column in cur:
        act_hero = None
        for hero in heroes:
            if hero['id'] == column[6]:
                act_hero = hero
                break

        if act_hero is None:

            act_hero = {}
            act_hero['id'] = column[6]
            act_hero['name'] = column[4]

            purchases = []

            purchase = {}
            purchase['id'] = column[8]
            purchase['name'] = column[0]
            purchase['count'] = column[9]
            purchases.append(purchase)

            act_hero['top_purchases'] = purchases

            heroes.append(act_hero)

        else:
            purchases = act_hero['top_purchases']

            purchase = {}
            purchase['id'] = column[8]
            purchase['name'] = column[0]
            purchase['count'] = column[9]
            purchases.append(purchase)


    dic['heroes'] = heroes

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