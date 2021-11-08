from db.config import TEST as DB_TEST
from db.user import *

def testTable():
    table = Table()
    table.db = DB_TEST
    table.trace = True
    table.drop()
    table.create()
    return table

def alice():
    return {
        COL_NAME: 'alice',
        COL_EMAIL: 'alice@peeps.net',
    }

def bob():
    return {
        COL_NAME: 'bob',
        COL_EMAIL: 'bob@peeps.net',
    }


def test_drop():
    with testTable() as table:
        table.drop()
        failed = False
        try:
            table.insert(alice())
        except:
            failed = True
        assert failed == True

def test_create():
    with testTable() as table:
        table.drop()
        table.create()
        failed = False
        try:
            table.insert(alice())
        except:
            failed = True
        assert failed == False

def test_insert():
    with testTable() as table:
        row1=alice()
        row2=bob()
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
        id=table.insert(alice())
        table.update({COL_ID:id,COL_NAME:'alicia'})
        row=table.row(id)
        assert row[COL_NAME] == 'alicia'
        assert row[COL_EMAIL] == alice()[COL_EMAIL]
