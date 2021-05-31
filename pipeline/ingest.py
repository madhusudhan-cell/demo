import logging
import logging.config
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as sqlio
from pipeline import configurationParser as cp


class Ingest:
    logging.config.fileConfig(cp.configuration("PATHS", "LOGGING_CONF"))

    def __init__(self, spark):
        self.spark = spark

    def ingest_hotel_data(self):
        logging.info("Ingesting from hotel data")
        try:
            hotel_csv = cp.configuration("PATHS", "HOTEL_CSV")
            hotel_df = self.spark.read\
                .csv(hotel_csv, header=True)
            logging.info("Hotel DF created")
            return hotel_df
        except Exception as exp:
            logging.error("Error occurred while ingesting the Hotel data > {}".format(exp))
            raise Exception("Ingesting hotel data failed")

    def ingest_expedia_data(self):
        logging.info("Ingesting from expedia data")
        try:
            expedia_avro = cp.configuration("PATHS", "EXPEDIA_AVRO")
            expedia_df = self.spark.read\
                .format("com.databricks.spark.avro")\
                .load("pipeline/input/expedia/")
            expedia_df.show()
            return expedia_df
        except Exception as exp:
            logging.error("Error occurred while ingesting Expedia data > {}".format(exp))
            raise Exception("Ingesting expedia data failed")

    def read_expedia_data_from_pg(self):
        logging.info("Ingesting expedia data from postgres")
        try:
            connection = pg.connect(user="postgres", password="admin", host="localhost", database="postgres")
            cursor = connection.cusrsor()
            sql_query = "select * from hotelRecommendation.expedia"
            expedia_pandas_df = sqlio.read_sql(sql_query, connection)
            expedia_spark_df = self.spark.createDataFrame(expedia_pandas_df)
            return expedia_spark_df
        except Exception as exp:
            logging.error("Error occurred while ingesting Expedia data from postgres> {}".format(exp))
            raise Exception("Ingesting expedia data from postgre failed")

