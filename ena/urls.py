import configparser
import os


class ConfigException(Exception):
    pass


def _raise_exception(name: str):
    raise ConfigException('{0} is not set in ena-urls.cfg'.format(name))


config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'ena-urls.cfg'))
test_server = config['submission']['test']
if not test_server:
    _raise_exception('submission/test')

production_server = config['submission']['production']
if not production_server:
    _raise_exception('submission/production')

ftp_server = config['ftp']['host']
if not ftp_server:
    _raise_exception('ftp/host')
