from flask import Flask, url_for, jsonify, request
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,inspect
from sqlalchemy import and_
import datetime as dt

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
inspector = inspect(engine)
inspector.get_table_names()
base = automap_base()
base.prepare(engine, reflect=True)
Measurement = base.classes.measurement
Station = base.classes.station

app = Flask(__name__)

@app.route("/")
def home():
    links = []
    for rule in app.url_map.iter_rules():
        if(rule.endpoint != 'static'):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            links.append((rule.endpoint, url))
    return jsonify(links)

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    measurements = session.query(Measurement).all()
    meas_dict = {measurement.date: measurement.prcp for measurement in measurements}
    session.close()
    return jsonify(meas_dict)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    stations = session.query(Station).all()
    stations_list = [station.station for station in stations]
    session.close()
    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    active_stations = session.query(Measurement.station, func.count(Measurement.station).label('number')).group_by(Measurement.station).all()
    session.close()
    max = active_stations[0].number
    max_station = active_stations[0].station
    for station in active_stations:
        if station.number > max:
            max_station = station.station
    results = session.query(Measurement).order_by(Measurement.date.desc())
    most_recent_date = results[0].date  
    most_recent_date_obj = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')
    last_year_obj = most_recent_date_obj - dt.timedelta(days=365)
    last_year_date = last_year_obj.strftime('%Y-%m-%d')
    measurements = session.query(Measurement).filter(and_(Measurement.date >= last_year_date)).all()
    measurement_list = [(measurement.id, measurement.station, measurement.date, measurement.prcp, measurement.tobs) for measurement in measurements]
    return jsonify(measurement_list)

@app.route("/api/v1.0/")
def stats():
    session = Session(engine)
    start_str = request.args.get('start')
    end_str = request.args.get('end')
    start_date = None
    end_date = None
    measurements = None
    if start_str is not None:
        start_date = dt.datetime.strptime(start_str, '%Y-%m-%d')
    else:
        raise Exception("start has to be provided!")

    if end_str is not None:
        end_date = dt.datetime.strptime(end_str, '%Y-%m-%d')
    partial_query = session.query(func.max(Measurement.tobs).label("max"), func.min(Measurement.tobs).label("min"), func.avg(Measurement.tobs).label("avg")).filter(Measurement.date.isnot(None)).filter(Measurement.tobs.isnot(None)).filter(Measurement.date >= start_date) 
    if end_date is not None:
        measurements = partial_query.filter(Measurement.date <= end_date).all()
    else: 
        measurements = partial_query.all()
    measurement_list = []
    for measurement in measurements:
        meas_dict = {}
        meas_dict['max'] = measurement.max
        meas_dict['min'] = measurement.min
        meas_dict['avg'] = measurement.avg
        measurement_list.append(meas_dict)
    
    return jsonify(measurement_list)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5007)



