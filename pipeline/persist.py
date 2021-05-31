import logging
import logging.config
from pipeline import configurationParser as cp


class Persist:
    logging.config.fileConfig(cp.configuration("PATHS", "LOGGING_CONF"))

    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def persist_hotel_data(hotel_transformed_df):
        logging.info("Persisting geocoded data")
        try:
            hotel_transformed_df.write.option("header", "true")\
                .csv(cp.configuration("PATHS", "HOTEL_OUTPUT"))
            logging.info("Data persisted")
        except Exception as exp:
            logging.error("Error occurred while persisting hotel data > {}".format(exp))
            raise Exception("Directory already exists")

    @staticmethod
    def persist_expedia_data(expedia_incremental_df):
        logging.info("Persisting expedia data")
        try:
            expedia_incremental_df.write.option("header", "true") \
                .csv(cp.configuration("PATHS", "EXPEDIA_OUTPUT"))
            logging.info("Expedia data persisted")
        except Exception as exp:
            logging.error("Error occurred while persisting expedia data > {}".format(exp))
            raise Exception("Error occurred while persisting expedia data")


