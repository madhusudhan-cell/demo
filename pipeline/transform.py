from pyspark.sql.functions import concat_ws, udf, md5, col
from pyspark.sql.types import FloatType, StructType, StructField, StringType
import logging
import logging.config
from pipeline import geocodeUdf as geo
from pipeline import configurationParser as cp


class Transform:
    logging.config.fileConfig(cp.configuration("PATHS", "LOGGING_CONF"))

    def __init__(self, spark):
        self.spark = spark

    def transform_hotel_data(self, hotel_df):
        logging.info("Transforming hotel data")
        try:
            hotel_df_with_complete_address = self.merge_address_attributes_in_hotel_data(hotel_df)
            geocode_address = self.udf_pre_requisites()
            hotel_exploded_df = self.geocode_hotel_data(geocode_address, hotel_df_with_complete_address)
            return hotel_exploded_df
        except Exception as exp:
            logging.error("Error occurred while transforming hotel data > {}".format(exp))
            raise Exception("Error occurred while transforming the data")

    def transform_expedia_data(self, expedia_df, expedia_postgres_df):
        logging.info("Transforming expedia data")
        try:
            expedia_md5_added = self.md5_dataframe_rows(self, expedia_df, "md5_source")
            expedia_postgres_md5_added = self.md5_dataframe_rows(self, expedia_postgres_df, "md5_target")
            incremental_data = self.get_expedia_incremental_data(self, expedia_md5_added, expedia_postgres_md5_added)
            return incremental_data
        except Exception as exp:
            logging.error("Error occurred while transforming hotel data > {}".format(exp))
            raise Exception("Error occurred while transforming the data")
        
    def udf_pre_requisites(self):
        logging.info("Starting Opencage API pre-requisites")

        try:
            schema = StructType([
                StructField("opencage_lat", FloatType()),
                StructField("opencage_long", FloatType()),
                StructField("opencage_geohash", StringType())
            ])
            geocode_address = udf(lambda x: geo.geocode(x), schema)
            self.spark.udf.register("geocode_address", geocode_address)
            return geocode_address
        except Exception as exp:
            logging.error("Error occurred in Opencage pre-requisites > {}".format(exp))
            raise Exception("Error occurred in Opencage pre-requisites")

    @staticmethod
    def merge_address_attributes_in_hotel_data(hotel_df):
        # hotels_with_no_lat_lon = hotel_df\
        #     .filter((hotel_df['Latitude'].isin(['', 'NA']))
        #             | (hotel_df['Longitude'].isin('', 'NA'))
        #             | (hotel_df['Latitude'].isNull())
        #             | (hotel_df['Longitude'].isNull()))

        logging.info("Merging name, address, city, "
                     "country to single column")
        try:
            hotel_df_with_complete_address = hotel_df.limit(2) \
                .withColumn("complete_address",
                            concat_ws(", ", hotel_df['Name'], hotel_df['Address'], hotel_df['City'], hotel_df['Country']))
            # hotel_df_with_complete_address.select("id", "Complete_address").show(truncate=False)
            return hotel_df_with_complete_address
        except Exception as exp:
            logging.error("Error occurred while merging address > {}".format(exp))
            raise Exception("Error occurred while merging address")

    @staticmethod
    def geocode_hotel_data(geocode_address, hotel_df_with_complete_address):
        logging.info("Geocoding addresses started")

        try:
            hotel_df_gecoded = hotel_df_with_complete_address \
                .withColumn("geocode", geocode_address('complete_address'))

            # hotel_df_gecoded \ .select('Id', 'Complete_address', 'Latitude', 'Longitude', 'geocode.opencage_lat',
            # 'geocode.opencage_long', 'geocode.opencage_geohash') \ .show(truncate=False)

            hotel_exploded_df = hotel_df_gecoded \
                .select('Id', 'Complete_address', 'Latitude', 'Longitude', 'geocode.opencage_lat',
                        'geocode.opencage_long', 'geocode.opencage_geohash')
            logging.info("Addresses geocoded and returning the same")
            return hotel_exploded_df
        except Exception as exp:
            logging.error("Error occurred while geocoding > {}".format(exp))
            raise Exception("Error occurred while geocoding")
