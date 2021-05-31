import sys

from pyspark.sql import SparkSession
import logging
import logging.config
from pipeline import transform, persist, ingest, configurationParser as cp


class Pipeline:
    logging.config.fileConfig(cp.configuration("PATHS", "LOGGING_CONF"))

    def run_pipeline(self):
        logging.info("Pipeline started")

        try:
            ingest_process = ingest.Ingest(self.spark)
            hotel_df = ingest_process.ingest_hotel_data()
            # expedia_df = ingest_process.ingest_expedia_data()
            # expedia_pg_df = ingest_process.read_expedia_data_from_pg()

            transform_process = transform.Transform(self.spark)
            hotel_transformed_df = transform_process.transform_hotel_data(hotel_df)
            # expedia_incremental_df = transform_process.transform_expedia_data(expedia_df, expedia_pg_df)

            persist_process = persist.Persist(self.spark)
            persist_process.persist_hotel_data(hotel_transformed_df)
            # persist_process.persist_expedia_data(expedia_incremental_df)

            logging.info("Pipeline completed")
            logging.info("Stopping spark session")
            self.spark.stop()
        except Exception as exp:
            logging.error("Error occured while running the pipeline > {}".format(exp))
            sys.exit(1)

    def create_spark_session(self):
        logging.info("Creating spark session")
        try:
            self.spark = SparkSession \
                .builder \
                .appName("Hotel Data") \
                .enableHiveSupport() \
                .getOrCreate()

            logging.info("Spark session created")
        except Exception as exp:
            logging.error("Error occured while creating Spark session > {}".format(exp))
            sys.exit(1)


if __name__ == "__main__":
    # logging.basicConfig(level="INFO")
    logging.info("Invoked from main method")

    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()

    logging.info("Exiting from main method")
