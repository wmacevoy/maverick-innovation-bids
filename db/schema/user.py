TABLE_NAME='user'
COL_ID='id'
COL_NAME='name'
COL_EMAIL='email'

def valerr(msg):
    raise ValueError(msg)

FILTERS = { 
    COL_ID: lambda x : int(x),
    COL_NAME: lambda x : str(x),
    COL_EMAIL: lambda x : str(x)}


CREATE = f"""
create table if not exists {TABLE_NAME} (
    {COL_ID} integer primary key,
    {COL_NAME} text,
    {COL_EMAIL} text)
"""
