import requests
import time

import database as db

DB_CON = db.connect_to_db()
DARK_SKY_API_KEY = ''


def seed():
    lat = 33.4242
    lon = -111.9281
    url = f'https://api.darksky.net/forecast/{DARK_SKY_API_KEY}/{lat},{lon}?'
    params = {'exclude': 'currently, minutely, daily, alerts, flags',
              'time': time.mktime(time.strptime("01.05.2020 00:00:01", "%d.%m.%Y %H:%M:%S"))}

    r = requests.get(url, params=params)
    if r.ok:
        data = r.json()

        obs = db.upsert_observer(
            DB_CON,
            "Darksky",
            "API Service",
            "",
        )

        for t in data['hourly']['data']:
            temp = db.insert_historic_data(
                DB_CON,
                obs,
                t['time'],
                db.format_lat_lon(lat, lon),
                t['temperature'],
                ""
            )
    print("Done")


if __name__ == '__main__':
    seed()