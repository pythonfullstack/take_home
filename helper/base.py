import math

import pandas as pd
from dateutil import parser


class Utils:
    @staticmethod
    def iso_8061_to_timestamp(time_str: str or None):
        """
        This function is for converting ISO-8061 format to timestamp
        :param time_str:
        :return: timestamp
        """
        return parser.parse(time_str)

    @staticmethod
    def load_db(connection, df, table_name="starlink_satellite", if_exists='replace', after_close=False):
        """
        This function is for loading dataframe data to database
        :param connection: DB connectoin instance
        :param df: dataframe
        :param table_name: the name of the table on the database
        :param if_exists: replace or append ...
        :param after_close: if True, connection instance will be closed.
        :return:
        """
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
        """
        This function is for reading json data and extracting data and return dataframe
        :param data_list: [{...}, {...}]
        :return: dataframe
        """
        filtered_data = []
        for data in data_list:
            creation_date = Utils.iso_8061_to_timestamp(data['spaceTrack']['CREATION_DATE'])
            longitude = data['longitude']
            latitude = data['latitude']
            satellite_id = data['launch']
            filtered_data.append([creation_date, longitude, latitude, satellite_id])
        return pd.DataFrame(filtered_data, columns=['creation_date', 'longitude', 'latitude', 'satellite_id'])

    @staticmethod
    def calc_distance(lon1, lat1, lon2, lat2):
        """
        This function is for calculating between 2 positions with following formula.
        	a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
            c = 2 ⋅ atan2( √a, √(1−a) )
            d = R ⋅ c
        :param lon1:
        :param lat1:
        :param lon2:
        :param lat2:
        :return:
        """
        radius = 637e3

        hai1 = lat1 * math.pi / 180
        hai2 = lat2 * math.pi / 180
        delta_hai = (lat2 - lat1) * math.pi / 180
        delta_lambda = (lon2 - lon1) * math.pi / 180

        a = math.sin(delta_hai / 2) * math.sin(delta_hai / 2) + math.cos(hai1) * math.cos(hai2) * math.sin(
            delta_lambda / 2) * math.sin(delta_lambda / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return radius * c
