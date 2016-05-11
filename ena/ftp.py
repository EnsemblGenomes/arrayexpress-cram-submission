import datetime
import ftplib
import os
import subprocess
from typing import List

import urls
from ena import credentials


def upload_to_ena(path: str):
    session = ftplib.FTP(urls.ftp_server, credentials.user, credentials.password)
    file_name = os.path.basename(path)
    with open(path, 'rb') as f:
        session.storbinary('STOR ' + file_name, f)
    session.quit()


def _remove_from_ena(path: str):
    session = ftplib.FTP(urls.ftp_server, credentials.user, credentials.password)
    file_name = os.path.basename(path)
    session.delete(file_name)
    session.quit()


def upload_to_ena_aspera(path: str):
    command = 'ascp -QT -l3000M --ignore-host-key {path} {user}@{url}:.'.format(path=path, user=credentials.user,
                                                                                url=urls.ftp_server)
    env = {'ASPERA_SCP_PASS': credentials.password}
    print('executing command', command)
    with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env) as proc:
        err = proc.stderr.read()
        if err:
            print(err)
            raise BrokenPipeError(err)


def is_present(file_name) -> bool:
    """Caching file lookup. Provided to avoid the overhead of checking for the existence of
    a large number of files one-by-one."""
    cache_key = urls.ftp_server + credentials.user
    now = datetime.datetime.now()
    seconds_since_update = (now - _Cache.last_updated).seconds

    if (cache_key not in _Cache.ftp_dir_content) or (seconds_since_update >= 5):
        _Cache.ftp_dir_content[cache_key] = _list_dir()
        _Cache._last_updated = datetime.datetime.now()
    cram_files = _Cache.ftp_dir_content[cache_key]
    return file_name in cram_files


class _Cache:
    ftp_dir_content = dict()
    last_updated = datetime.datetime.now()


def _list_dir() -> List[str]:
    session = ftplib.FTP(urls.ftp_server, credentials.user, credentials.password)
    files = session.nlst()
    session.quit()
    cram_files = list(filter(lambda f: f.endswith('.cram'), files))
    return cram_files
