from flask import Flask, jsonify

import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

#Database setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

#reflect existing database into a new model
Base = automap_base()

#reflect the tables
Base.prepare(engine, reflect = True)

#save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

#flask setup
app = Flask(__name__)

#flask routes

#convert query results into a dictionary using key 'date' and value 'prcp'

#return json representation of dictionary
@app.route("/api/v1.0/precipitation")
def precipitation():
    
    session = Session(engine)
    
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest_date = most_recent_date.date
    
    a_year_before = dt.date.fromisoformat(latest_date) - dt.timedelta(days=365)
    
    in_between = session.query(Measurement.date, func.sum(Measurement.prcp)).group_by(Measurement.date).\
        filter(Measurement.date >= a_year_before).all()
    
    session.close()
    
    prcp_dict = {}
    prcp_list = []
    
    for date, prcp in in_between:
        prcp_dict[date] = prcp
        prcp_list.append(prcp_dict)
    
    return jsonify(prcp_list)

#return a json list of stations from the dataset
@app.route("/api/v1.0/stations")
def stations():
    
    session = Session(engine)
    
    station_query = session.query(Station.id, Station.elevation, Station.latitude, Station.longitude, Station.station, Station.name).all()
    
    session.close()
    
    
    station_list = []
    
    for st_id, elev, lat, long, station, name in station_query:
        station_dict = {}
        station_dict["id"] = st_id
        station_dict["elevation"] = elev
        station_dict["latitude"] = lat
        station_dict["longitude"] = long
        station_dict["station"] = station
        station_dict["name"] = name
        
        station_list.append(station_dict)
    
    return jsonify(station_list)

#query the dates and temperature of the most active station for the last year of data
#return a json list of temperature observations for the previous year
@app.route("/api/v1.0/tobs")
def tobs():
    
    session = Session(engine)
    
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest_date = most_recent_date.date
    
    a_year_before = dt.date.fromisoformat(latest_date) - dt.timedelta(days=365)
    
    get_active_station = session.query(Measurement.station, func.count(Measurement.id)).group_by(Measurement.station).\
                order_by(func.count(Measurement.id).desc()).first()
    
    active_station_name = get_active_station.name
    
    active_station_query = session.query(Measurement.station, Measurement.date, Measurement.tobs).filter(Measurement.station == active_station_name).filter(Measurement.date >= a_year_before).all()
    
    session.close()
    
    active_station_list = []
    for station, date, t_obs in active_station_query:
        active_station_dict = {}
        active_station_dict["station"] = station
        active_station_dict["date"] = date
        active_station_dict["tobs"] = t_obs
        
        active_station_list.append(active_station_dict)
        
    return jsonify(active_station_list)

#return a json list of the minimum temperature, avg temperature, and max temperature of a given start or start-end
#if start only, calculate only for dates greater than or equal to the start date
#if start-end, calculate for dates between the start and end date INCLUSIVE

@app.route("/api/v1.0/<start>")
def start(start_date):
    
    session = Session(engine)
    
    get_first_date = session.query(Measurement.date).order_by(Measurement.date.asc()).first()
    first_date = get_first_date.date
    
    get_last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_date = get_last_date.date
    
    session.close()
    
    if(start_date[0:4].isnumeric() and start_date[4] == "-" and start_date[5:7].isnumeric() and start_date[7] == "-" and start_date[8:10].isnumeric()):
    
        if(start_date >= first_date and start_date <= last_date):
            session = Session(engine)
            greater_than_equal = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).all()

            session.close()

            tobs_list = []

            for min_tobs, mean_tobs, max_tobs in greater_than_equal:
                tobs_dict = {}
                tobs["start date"] = start_date
                tobs["end date"] = last_date
                tobs["minimum temperature"] = min_tobs
                tobs["average temperature"] = mean_tobs
                tobs["maximum temperature"] = max_tobs

                tobs_list.append(tobs_dict)

            return jsonify(tobs_list)

        return jsonify({"error":f"Invalid date entered. Date must be between {first_date} and {last_date}"})
    
    return jsonify({"error":f"Invalid date format. Date must be represented as: 'YYYY-M-DD'."})
    

@app.route("/api/v1.0/<start>/<end>")
def start_and_end(start_date, end_date):
    
    session = Session(engine)
    
    get_first_date = session.query(Measurement.date).order_by(Measurement.date.asc()).first()
    first_date = get_first_date.date
    
    get_last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_date = get_last_date.date
    
    session.close()
    
    if(start_date[0:4].isnumeric() and start_date[4] == "-" and start_date[5:7].isnumeric() and start_date[7] == "-" and start_date[8:10].isnumeric() and end_date[0:4].isnumeric() and end_date[4] == "-" and end_date[5:7].isnumeric() and end_date[7] == "-" and end_date[8:10].isnumeric()):
    
        if(start_date >= first_date and start_date <= last_date and end_date >= first_date and end_date <= last_date):
            session = Session(engine)
            greater_than_equal = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).filter(Measurement.data <= end_date).all()

            session.close()

            tobs_list = []

            for min_tobs, mean_tobs, max_tobs in greater_than_equal:
                tobs_dict = {}
                tobs["start date"] = start_date
                tobs["end date"] = end_date
                tobs["minimum temperature"] = min_tobs
                tobs["average temperature"] = mean_tobs
                tobs["maximum temperature"] = max_tobs

                tobs_list.append(tobs_dict)

            return jsonify(tobs_list)

        return jsonify({"error":f"Invalid date entered. Both dates must be between {first_date} and {last_date}"})
    
    return jsonify({"error":f"Invalid date format. Dates must be represented as: 'YYYY-M-DD'."})
    
    
    return jsonify()

@app.route("/")
def welcome():
    
    session = Session(engine)
    
    get_first_date = session.query(Measurement.date).order_by(Measurement.date.asc()).first()
    first_date = get_first_date.date
    
    get_last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_date = get_last_date.date
    
    session.close()
    
    
    return (
        f"Availible Routes: <br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br>"
        f"/api/v1.0/<start>/<end>"
    )
    

if __name__ == '__main__':
    app.run(debug=True)