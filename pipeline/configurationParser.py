from configparser import ConfigParser


def configuration(section, key):
    parser = ConfigParser()
    parser.read("pipeline/resources/pipeline.ini")
    return parser.get(section, key)