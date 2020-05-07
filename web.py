import utilities
import database as db

from flask import Flask, request, jsonify
app = Flask(__name__)


DB_CON = db.connect_to_db()


@app.route("/cs/observation", methods=['POST'])
def new_data():
    point = request.args.get('point', None)
    place = request.args.get('place', None)
    data_note = ""

    observer = request.args.get('observer', 'anon')
    obs_type = request.args.get('obs_type', 'REST')

    temp = request.args.get('temp')

    if not utilities.is_float(temp):
        return "No valid temperature provided"

    if point is None and place is not None:
        point = utilities.coord_from_str(place)
        data_note = f"point extrapolated from: {place}"

    if point is None:
        return "No location information provided"
    elif isinstance(point, str) and place is None:
        p = point.split(',')
        point = [p[0], p[1]]

    if point is not None:
        obs = db.upsert_observer(
            DB_CON,
            observer,
            obs_type,
            "",
        )

        temp = db.insert_data(
            DB_CON,
            obs,
            db.format_point(point),
            temp,
            data_note
        )
        return f"Successfully recorded temperate observation #{temp[0]}"

    return "Unexpected parameter configuration, ignoring input"


@app.route("/cs/getJoinedData", methods=['GET'])
def get_all_joined():
    return jsonify(db.get_data(DB_CON))


if __name__ == '__main__':
    app.run()
