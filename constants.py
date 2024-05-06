import os



_DATABASE_UID = os.environ.get('DATABASE_UID', '')
_DATABASE_PWD = os.environ.get('DATABASE_PWD', '')
_DATABASE_HOST = os.environ.get('_DATABASE_HOST', '')

def get_db_url(uid, pwd, server, port='5432', driver='postgresql', database='sports_data'):
    url = f"{driver}://{uid}:{pwd}@{server}:{port}/{database}"
    return url

_DB_URL = get_db_url(uid=_DATABASE_UID, pwd=_DATABASE_PWD, server=_DATABASE_HOST)

