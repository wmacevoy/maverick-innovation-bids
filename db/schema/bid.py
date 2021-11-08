
TABLE_NAME='bid'
COL_ID='id'
COL_TIMESTAMP='timestamp'
COL_USER_ID='userId'
COL_ITEM_ID='itemId'
COL_OFFER='offer'

FILTERS = { 
    COL_ID: lambda x : int(x),
    COL_TIMESTAMP: lambda x : str(x),
    COL_USER_ID: lambda x : int(x),
    COL_ITEM_ID: lambda x : int(x),
    COL_OFFER: lambda x: float(x) }

CREATE=f"""
create table if not exists {TABLE_NAME} (
    {COL_ID} integer primary key,
    {COL_TIMESTAMP} text,
    {COL_USER_ID} integer,
    {COL_ITEM_ID} integer,
    {COL_OFFER} float)
"""
