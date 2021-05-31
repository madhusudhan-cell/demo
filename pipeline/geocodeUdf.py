import sys
from opencage.geocoder import OpenCageGeocode, RateLimitExceededError, InvalidInputError
import logging
import logging.config
from pipeline import configurationParser as cp

logging.config.fileConfig(cp.configuration("PATHS", "LOGGING_CONF"))


def geocode(address):
    # print("Geocoding hotel data")
    key = cp.configuration("KEYS", "OPEN_CAGE")
    geocoder = OpenCageGeocode(key)

    try:
        result = geocoder.geocode(address.strip())
        if result:
            longitude = result[0]['geometry']['lng']
            latitude = result[0]['geometry']['lat']
            geohash = result[0]['annotations']['geohash']
            # print(u'%f;%f;%s;%s' % (latitude, longitude, geohash, address))
            # 40.416705;-3.703582;Madrid,Spain
            return float(latitude), float(longitude), str(geohash)
        else:
            sys.stderr.write("not found: %s\n" % address)
    except RateLimitExceededError as exp:
        logging.error("Error occured while using Opencage > {}".format(exp))
        raise Exception("Error while using Opencage")
        # Your rate limit has expired. It will reset to 2500 on 2020-10-08T00:00:00
        # Upgrade your plan on https://opencagedata.com/pricing or wait until
        # the next day https://opencagedata.com/clock
    except InvalidInputError as exp:
        logging.error("Invalid input error while using Opencage > {}".format(exp))
        raise Exception("Error while using Opencage")
        # this happens for example with invalid unicode in the input data