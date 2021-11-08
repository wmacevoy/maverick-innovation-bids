from db.config import TEST as DB_TEST
from db.bid import *
import db.item
import db.user

def pencil():
    return {
        db.item.COL_NAME: 'pencil',
        db.item.COL_DESCRIPTION: 'straight',
        db.item.COL_QUANTITY: 33,
        db.item.COL_MIN_OFFER: 0.1,
    }

def paper():
    return {
        db.item.COL_NAME: 'paper',
        db.item.COL_DESCRIPTION: 'flat',
        db.item.COL_QUANTITY: 1,
        db.item.COL_MIN_OFFER: 0.01,
    }


def alice():
    return {
        db.user.COL_NAME: 'alice',
        db.user.COL_EMAIL: 'alice@peeps.net',
    }

def bob():
    return {
        db.user.COL_NAME: 'bob',
        db.user.COL_EMAIL: 'bob@peeps.net',
    }

TRACE = True

def testUser(empty = False):
    user = db.user.Table()
    user.db = DB_TEST
    user.trace = TRACE
    user.drop()
    user.create()
    if not empty:
        user.insert(alice())
        user.insert(bob())
    return user

USER_ID={'alice': 1, 'bob':2}

def testItem(empty = False):
    item = db.item.Table()
    item.db = DB_TEST
    item.trace = TRACE
    item.drop()
    item.create()
    if not empty:
        item.insert(paper())
        item.insert(pencil())
    return item

ITEM_ID={'paper' : 1, 'pencil' : 2}


def bid(ts,offer,user,item):
    secs = ts % 60
    mins = ts // 60
    timestamp="2021-11-01T00:%02d:%02d" % (mins,secs)
    return {
        'timestamp': timestamp,
        'offer': offer,
        'userId': USER_ID[user],
        'itemId': ITEM_ID[item]
    }

BID=[
    bid( 0,1.00,"alice","pencil"),
    bid( 1,1.00,"bob","pencil"),
    bid( 2,1.05,"alice","pencil"),
    bid( 3,1.05,"bob","paper"),
]

def testTable(empty = False):
    table = Table()
    table.db = DB_TEST
    table.trace = TRACE
    table.drop()
    table.create()
    if not empty:
        for bid in BID:
            table.insert(bid)
    return table


def test_drop():
    with testTable(True) as table:
        table.drop()
        failed = False
        try:
            table.insert(BID[0])
        except:
            failed = True
        assert failed == True

def test_create():
    with testTable(True) as table:
        table.drop()
        table.create()
        failed = False
        try:
            table.insert(BID[0])
        except:
            failed = True
        assert failed == False

def test_insert():
    with testTable(True) as table,testUser() as user, testItem() as item:
        rows = []
        for bid in BID:
            id=table.insert(bid)
            row=bid.copy()
            row[COL_ID]=id
            rows.append(row)

        for row in rows:
            ids=table.selectIdsByTimestampUserItem(
                row[COL_TIMESTAMP],row[COL_USER_ID],row[COL_ITEM_ID])
            assert len(ids) == 1
            assert len(ids[0]) == 1
            assert ids[0][0] == row[COL_ID]

            selRow=table.row(row[COL_ID])
            for col in FILTERS:
                assert row[col] == selRow[col]

def test_update():
    with testTable() as table,testUser() as user, testItem() as item:
        id = 2
        table.update({COL_ID:id,COL_OFFER:3.14})
        row=table.row(id)
        assert row[COL_OFFER] == 3.14
