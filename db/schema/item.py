TABLE_NAME='item'
COL_ID='id'
COL_NAME='name'
COL_QUANTITY='quantity'
COL_DESCRIPTION='description'
COL_MIN_OFFER='minOffer'

def valerr(msg):
    raise ValueError(msg)

FILTERS = { 
    COL_ID: lambda x : int(x),
    COL_NAME: lambda x : str(x),
    COL_QUANTITY: lambda x : int(x) if int(x) >= 0 else valerr("negative"),
    COL_DESCRIPTION: lambda x : str(x),
    COL_MIN_OFFER: lambda x : float(x) if float(x) >= 0 else valerr("negative")
    }
