import configparser
import os


class ConfigException(Exception):
    pass


def _raise_exception(name: str):
    raise ConfigException('{0} is not set in urls.cfg'.format(name))


_config = configparser.ConfigParser()
_config.read(os.path.join(os.path.dirname(__file__), '..', 'urls.cfg'))

test_server = _config['ENA submission']['test']
if not test_server:
    _raise_exception('ENA submission/test')

production_server = _config['ENA submission']['production']
if not production_server:
    _raise_exception('ENA submission/production')

ftp_server = _config['ENA ftp']['host']
if not ftp_server:
    _raise_exception('ENA ftp/host')

sqlite = _config['result DB']['sqlite']
