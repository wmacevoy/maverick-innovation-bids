from db.config import TEST as DB_TEST
from db.item import *

def testTable():
    table = Table()
    table.db = DB_TEST
    table.trace = True
    table.drop()
    table.create()
    return table

def pencil():
    return {
        COL_NAME: 'pencil',
        COL_DESCRIPTION: 'straight',
        COL_QUANTITY: 33,
        COL_MIN_OFFER: 0.1,
    }

def paper():
    return {
        COL_NAME: 'paper',
        COL_DESCRIPTION: 'flat',
        COL_QUANTITY: 1,
        COL_MIN_OFFER: 0.01,
    }


def test_drop():
    with testTable() as table:
        table.drop()
        failed = False
        try:
            table.insert(pencil())
        except:
            failed = True
        assert failed == True

def test_create():
    with testTable() as table:
        table.drop()
        table.create()
        failed = False
        try:
            table.insert(pencil())
        except:
            failed = True
        assert failed == False

def test_insert():
    with testTable() as table:
        row1=paper()
        row2=pencil()
        id1=table.insert(row1)
        row1[COL_ID]=id1
        id2=table.insert(row2)
        row2[COL_ID]=id2

        for row in [row1,row2]:
            ids=table.selectIdsByName(row[COL_NAME])
            assert len(ids) == 1
            assert len(ids[0]) == 1
            assert ids[0][0] == row[COL_ID]

            selRow=table.row(row[COL_ID])
            for col in FILTERS:
                assert row[col] == selRow[col]

def test_update():
    with testTable() as table:
        id=table.insert(pencil())
        table.update({COL_ID:id,COL_NAME:'pens',COL_DESCRIPTION:'black'})
        row=table.row(id)
        assert row[COL_NAME] == 'pens'
        assert row[COL_DESCRIPTION] == 'black'
        assert row[COL_QUANTITY] == pencil()[COL_QUANTITY]
