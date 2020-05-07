import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import utilities

DATABASE = 'crowd_station'
USER = 'postgres'
PASSWORD = 'elephant'
HOST = 'localhost'
DATA_TBL = 'observation'
USER_TBL = 'observer'
COORD_SYS = 4326


def create_db():
    con = psycopg2.connect(host=HOST, user=USER, password=PASSWORD)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Create database doesn't have if not exists
    cur.execute(sql.SQL('SELECT datname FROM pg_catalog.pg_database WHERE datname = {db_name};').format(
        db_name=sql.Literal(DATABASE)
    ))

    if cur.fetchone() is None:
        cur.execute(sql.SQL('CREATE DATABASE {db_name};').format(
            db_name=sql.Identifier(DATABASE)
        ))
    else:
        print(f'DB {DATABASE} already exists')
    cur.close()
    con.close()


def connect_to_db():
    con = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return con


# Create PostGIS extension
def create_extn(connection):
    cur = connection.cursor()
    cur.execute(sql.SQL('CREATE EXTENSION IF NOT EXISTS postgis;').format())
    cur.close()


# Create tables
def create_tables(connection):
    cur = connection.cursor()
    cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {tbl} ('
                        'id SERIAL PRIMARY KEY, '
                        'alias TEXT CONSTRAINT unique_name UNIQUE, '
                        'obs_type TEXT,'
                        'note TEXT);').format(
        tbl=sql.Identifier(USER_TBL)
    ))

    cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {tbl} ('
                        'id SERIAL PRIMARY KEY, '
                        'source INT REFERENCES {src}(id), '
                        'ts TIMESTAMP DEFAULT now(), '
                        'location GEOGRAPHY, '
                        'temperature DECIMAL, '
                        'note TEXT);').format(
        tbl=sql.Identifier(DATA_TBL),
        src=sql.Identifier(USER_TBL)
    ))

    cur.execute(sql.SQL('CREATE INDEX loc_index ON {tbl} USING GIST(location);').format(
        tbl=sql.Identifier(DATA_TBL)))

    cur.close()


def upsert_observer(connection, name, obs_type, note=""):
    cur = connection.cursor()
    cur.execute(sql.SQL('INSERT INTO {tbl} '
                        '(alias, obs_type, note) '
                        'VALUES ({alias}, {obs_type}, {note}) '
                        'ON CONFLICT ON CONSTRAINT unique_name '
                        'DO UPDATE '
                        'SET alias = EXCLUDED.alias, obs_type = EXCLUDED.obs_type, note = EXCLUDED.note '
                        'RETURNING id;').format(
        tbl=sql.Identifier(USER_TBL),
        alias=sql.Literal(name),
        obs_type=sql.Literal(obs_type),
        note=sql.Literal(note)
    ))
    var = cur.fetchone()[0]
    cur.close()

    return var


def insert_data(connection, source_id, loc_wkt, temp, note=""):
    cur = connection.cursor()
    cur.execute(sql.SQL('INSERT INTO {tbl} '
                        '(source, location, temperature, note) '
                        'VALUES ({source_id}, ST_GeomFromText({wkt}, {coord}), {temp}, {note}) '
                        'RETURNING id;').format(
        tbl=sql.Identifier(DATA_TBL),
        source_id=sql.Literal(source_id),
        wkt=sql.Literal(loc_wkt),
        coord=sql.Literal(COORD_SYS),
        temp=sql.Literal(temp),
        note=sql.Literal(note)
    ))
    var = cur.fetchone()[0]
    cur.close()

    return var


def insert_historic_data(connection, source_id: int, ts: int, loc_wkt: str, temp: float, note=""):
    cur = connection.cursor()
    cur.execute(sql.SQL('INSERT INTO {tbl} '
                        '(source, ts, location, temperature, note) '
                        'VALUES ({source_id}, to_timestamp({ts}), ST_GeomFromText({wkt}, {coord}), {temp}, {note}) '
                        'RETURNING id;').format(
        tbl=sql.Identifier(DATA_TBL),
        source_id=sql.Literal(source_id),
        ts=sql.Literal(ts),
        wkt=sql.Literal(loc_wkt),
        coord=sql.Literal(COORD_SYS),
        temp=sql.Literal(temp),
        note=sql.Literal(note)
    ))
    var = cur.fetchone()
    cur.close()

    return var


def format_lat_lon(lat, lon):
    lat = utilities.lat_to_float(lat)
    lon = utilities.lon_to_float(lon)

    return f"point({lon} {lat})"


def format_point(point):
    return format_lat_lon(point[0], point[1])


def get_data(connection):
    cur = connection.cursor()
    # ideally the cast would override the native return, but since it doesn't all values are listed
    cur.execute(sql.SQL('SELECT t.id, t.source, t.ts, t.location, t.temperature::FLOAT, t.note '
                        'FROM {tbl} t;').format(
        tbl=sql.Identifier(DATA_TBL)
    ))
    var = cur.fetchall()
    cur.close()
    return var
