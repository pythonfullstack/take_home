import argparse
import json
import requests
import pandas as pd
from sqlalchemy import create_engine

from helper.base import Utils

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Input querying parameters.")

    parser.add_argument('--id', default="5fbfecce54ceb10a5664c80a", help="satellite id", type=str)
    parser.add_argument('--datetime', default="2021-01-26T06:26:10", help="datetime", type=str)
    parser.add_argument('--longitude', default=100, help="longitude", type=int)
    parser.add_argument('--latitude', default=100, help="latitude", type=int)
    query_param = vars(parser.parse_args())

    # given condition for querying
    satellite_id = query_param['id']
    date_time = query_param['datetime']

    # db instance
    alchemy_engine = create_engine('postgresql+psycopg2://postgres:changeme@172.18.0.3/postgres', pool_recycle=3600)
    postgre_SQL_connection = alchemy_engine.connect()

    # read json file
    with open("starlink_historical_data.json", 'r') as file:
        json_data = json.load(file)

    # loading data into db
    # Task 2
    Utils.load_db(connection=postgre_SQL_connection, df=Utils.read_data(json_data))

    # Task 3  without cleaning db
    # by using sql alchemy

    # define query
    query = f"SELECT * FROM starlink_satellite WHERE satellite_id='{satellite_id}' "
    if date_time is not None:
        query += f"AND creation_date='{Utils.iso_8061_to_timestamp(date_time)}'::timestamptz "
    query += "ORDER BY creation_date LIMIT 1"

    longitude, latitude, date = pd.read_sql(query, postgre_SQL_connection).loc[
        0, ['longitude', 'latitude', 'creation_date']]

    print("Task 3")
    print(f"satellite_id: {satellite_id}, datetime: {date}")
    print(f"Longitude: {longitude}, latitude: {latitude}")

    # Task 4
    # given position and datetime
    given_position = {
        'longitude': query_param['longitude'],
        'latitude': query_param['latitude']
    }
    date_time = query_param['datetime']

    # define query
    query = f"SELECT * FROM starlink_satellite "
    if date_time is not None:
        query += f"WHERE creation_date='{Utils.iso_8061_to_timestamp(date_time)}'::timestamptz"

    # extract data from database
    data_df = pd.read_sql(query, postgre_SQL_connection)

    # remove rows containing NaN
    data_df.dropna(inplace=True)

    distance_list = []  # list containing distance data
    for row in data_df.iloc():
        distance_list.append(Utils.calc_distance(row['longitude'], row['latitude'], given_position['longitude'],
                                                 given_position['latitude']))
    data_df['distance'] = distance_list  # create a new column with the distance list

    print("Task 4")
    print(data_df[data_df.distance == data_df.distance.min()])
