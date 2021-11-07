
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
