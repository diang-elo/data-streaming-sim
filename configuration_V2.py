from configparser import ConfigParser

config = ConfigParser()
file = "./configuration_V2.ini"
config.read(file)

def get_value_config(header,key):
    """
    Args: header --> header in config.ini
            key --> key in header from config.ini
    
    returns the value from a given header and key in configuration file
    """
    return config[header][key]


def get_file_config(key):
    """
    returns value of key in simulation header of config ini
    """
    return config["simulator"][key]
