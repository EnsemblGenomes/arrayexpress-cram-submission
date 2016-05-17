"""Read ENA username and password from the environment."""
import os


class EnvironmentException(Exception):
    pass


def _raise_exception(name: str):
    raise EnvironmentException('Environment variable {0} is not set.'.format(name))


user = os.environ.get('ena_user')
if not user:
    _raise_exception('ena_user')

password = os.environ.get('ena_password')
if not password:
    _raise_exception('ena_password')
