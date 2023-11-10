import os
import logging
import requests
import pandas as pd
from datetime import datetime
from snowflake.sqlalchemy import URL

from sqlalchemy import create_engine
from sqlalchemy.types import DateTime
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import pd_writer


load_dotenv()
logger = logging.getLogger(__name__)
WEATHER_APPID = os.getenv('OPEN_WEATHER_API_KEY')

SNOWFLAKE_DATABASE = 'ORCA_TRACKER'
SNOWFLAKE_SCHEMA = 'WEATHER'
SNOWFLAKE_ROLE = 'ORCA_TRACKER_DEV'
SNOWFLAKE_WAREHOUSE = 'SPCS_ETL'
WEATHER_TABLE_NAME = 'TEMPERATURES'


def get_login_token():
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def get_snowflake_engine(snowflake_database, snowflake_schema, snowflake_warehouse, snowflake_role):
    if os.path.exists("/snowflake/session/token"):
        return create_engine(URL(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            host=os.getenv('SNOWFLAKE_HOST'),
            authenticator='oauth',
            token=get_login_token(),
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        ))
    else:
        return create_engine(URL(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role=snowflake_role,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        ))


class WeatherToSnowflake(object):
    base_geocoder_url = 'http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={weather_apikey}'
    base_weather_url = 'http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={weather_apikey}'

    def __init__(self, snowflake_engine, cities):
        self.snowflake_engine = snowflake_engine
        if not isinstance(cities, list):
            cities = [cities]
        self.cities = cities
        self._errors = []
        self.data = None

    def __call__(self, *args, **kwargs):
        logger.info('Extracting...')
        self.get_weather_data()

        logger.info('Transforming...')
        self.transform()

        logger.info('Loading...')
        self.load_df_to_sf()

        logger.info('Disposing of Snowflake Engine')
        self.close_connection()

    def close_connection(self):
        self.snowflake_engine.dispose()

    def log_error(self, stage, obj, msg):
        self._errors.append([stage, obj, msg])

    def get_weather_data(self):
        geocode_cities = []
        for city in self.cities:
            logger.info('Geocoding City: %s', city)
            geo_url = self.base_geocoder_url.format(city=city, weather_apikey=WEATHER_APPID)
            resp = requests.get(geo_url)
            if resp.status_code == 200 and resp.json():
                geo_resp = resp.json()[0]
                geocode_cities.append({
                    'country': geo_resp['country'],
                    'state': geo_resp['state'],
                    'city': city,
                    'lat': geo_resp['lat'],
                    'lon': geo_resp['lon']
                })
            else:
                logger.error('Failed to geocode %s', city)
                self.log_error(stage='Extract', obj=city, msg=f'Status Code: {resp.status_code}. Could not geolocate.')
        weather = []
        for city_details in geocode_cities:
            logger.info('Fetching weather: %s', city_details['city'])
            weather_url = self.base_weather_url.format(lat=city_details['lat'], lon=city_details['lon'],
                                                       weather_apikey=WEATHER_APPID)
            resp = requests.get(weather_url)
            if resp.status_code == 200 and resp.json():
                weather_resp = resp.json()
                weather.append({
                    # 'DATETIME': pd.to_datetime(weather_resp['dt'], unit='s'),
                    'DATETIME': datetime.utcfromtimestamp(weather_resp['dt']).strftime('%Y-%m-%d %H:%M:%S'),
                    'TEMP': weather_resp['main']['temp'],
                    'TEMP_HIGH': weather_resp['main']['temp_max'],
                    'TEMP_LOW': weather_resp['main']['temp_min'],
                    'COUNTRY': city_details['country'],
                    'STATE': city_details['state'],
                    'CITY': city_details['city'],
                    'LAT': city_details['lat'],
                    'LON': city_details['lon']
                })
            else:
                logger.error('Failed to fetch weather data %s', city_details['city'])
                self.log_error(
                    stage='Extract', obj=city_details['city'],
                    msg=f'Status Code: {resp.status_code}. Could not get weather.'
                )
        self.data = pd.DataFrame(weather)
        logger.info('%s Results Found', len(self.data))
        logger.info('%s total errors', len(self._errors))

    def transform(self):
        # TODO: Write what it would look like to change units based on country
        pass

    def load_df_to_sf(self):
        self.data.to_sql(name=WEATHER_TABLE_NAME, con=self.snowflake_engine, if_exists='append',
                         index=False, method=pd_writer, dtype={'DATETIME': DateTime})
        logger.info('Loaded %s Rows', len(self.data))


def main():
    snowflake_engine = get_snowflake_engine(
        snowflake_database=SNOWFLAKE_DATABASE,
        snowflake_schema=SNOWFLAKE_SCHEMA,
        snowflake_warehouse=SNOWFLAKE_WAREHOUSE,
        snowflake_role=SNOWFLAKE_ROLE
    )
    wts = WeatherToSnowflake(snowflake_engine=snowflake_engine, cities=['Montreal', 'New York', 'Chicago'])
    wts()
    logger.info('Done!')


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    main()
