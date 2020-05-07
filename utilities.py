import requests
import re

BING_MAPS_KEY = ''


def coord_from_str(location: str, result: int = 0):
    url = 'http://dev.virtualearth.net/REST/v1/Locations?'
    params = {'query': location, 'key': BING_MAPS_KEY}
    r = requests.get(url, params=params)

    pt = None
    if r.ok:
        data = r.json()
        pt = data['resourceSets'][0]['resources'][result]['point']['coordinates']
    else:
        print(f'Bing maps api error: {r}')
    return pt


def is_float(num):
    if num is not None:
        try:
            num = float(num)
            return True
        except ValueError:
            pass
    return False


def lat_to_float(lat):
    if isinstance(lat, str):
        lat = lat.lower()
        lat_l = re.findall("\d+\.\d+", lat)

        lat_f = 1.0
        if 's' in lat:
            lat_f = -1.0
        elif 'n' in lat:
            lat_f = 1.0

        return lat_f * abs(float(lat_l[0]))
    return lat


def lon_to_float(lon):
    if isinstance(lon, str):
        lon = lon.lower()

        lon_l = re.findall("\d+\.\d+", lon)

        lon_f = 1.0
        if 'e' in lon:
            lon_f = -1.0
        elif 'w' in lon:
            lon_f = 1.0

        return lon_f * abs(float(lon_l[0]))
    return lon
