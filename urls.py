import configparser
import os


class ConfigException(Exception):
    pass


_config = configparser.ConfigParser()
_config.read(os.path.join(os.path.dirname(__file__), 'urls.cfg'))

try:
    test_server = _config['ENA submission']['test']
    production_server = _config['ENA submission']['production']
    ftp_server = _config['ENA ftp']['host']
    sqlite = _config['result DB']['sqlite']
    arrayexpress = _config['ArrayExpress']['api']
except KeyError as e:
    raise ConfigException('{0} is not set in urls.cfg'.format(e.args[0])) from None
