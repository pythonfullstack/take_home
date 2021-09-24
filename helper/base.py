import math

import pandas as pd
from dateutil import parser


class Utils:
    @staticmethod
    def iso_8061_to_timestamp(time_str: str or None):
        return parser.parse(time_str)

    @staticmethod
    def load_db(connection, df, table_name="starlink_satellite", if_exists='replace', after_close=False):
        try:
            df.to_sql(table_name, connection, if_exists=if_exists)
            print("All data has been loaded successfully.")
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        else:
            print("PostgreSQL Table %s has been created successfully." % table_name)
        finally:
            if after_close:
                connection.close()

    @staticmethod
    def read_data(data_list: list) -> pd.DataFrame:
        filtered_data = []
        for data in data_list:
            creation_date = Utils.iso_8061_to_timestamp(data['spaceTrack']['CREATION_DATE'])
            longitude = data['longitude']
            latitude = data['latitude']
            satellite_id = data['launch']
            filtered_data.append([creation_date, longitude, latitude, satellite_id])
        return pd.DataFrame(filtered_data, columns=['creation_date', 'longitude', 'latitude', 'satellite_id'])

    @staticmethod
    def calc_distance(x1, y1, x2, y2):
        return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)
