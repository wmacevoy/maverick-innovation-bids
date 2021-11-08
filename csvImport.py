#!/usr/bin/env python

import sys,csv
import db.config
import db.item
import db.user
import db.bid
import googleform2isoformat

ALIASES = {
    'timestamp' : ['bid.timestamp','Timestamp'],
    'user.name' : ['user.name','What is your name?'],
    'user.email' : ['user.email','What is your Colorado Mesa University email address?'],
    'item.name' : ['item.name','What are you bidding for (one item per submission)'],
    'offer' : ['bid.offer','What is your offer for this item?']
}

def createTables(dbName=db.config.DEFAULT,trace=False):
    if (trace): print(f"createTables({dbName},{trace})")
    with db.bid.Table() as table:
        table.db = dbName
        table.trace = trace
        table.create()

    with db.item.Table() as table:
        table.db = dbName
        table.trace = trace
        table.create()
        table.csvImport('db/item.csv')

    with db.user.Table() as table:
        table.db = dbName
        table.trace = trace
        table.create()


def dropTables(dbName=db.config.DEFAULT,trace=False):
    if (trace): print(f"dropTables({dbName},{trace})")
    with db.bid.Table() as table:
        table.db = dbName
        table.trace = trace
        table.drop()

    with db.item.Table() as table:
        table.db = dbName
        table.trace = trace
        table.drop()

    with db.user.Table() as table:
        table.db = dbName
        table.trace = trace
        table.drop()

def csvFormImport(csvFileName,dbName=db.config.DEFAULT,trace=False):
    if (trace): print(f"csvFormImport({csvFileName},{dbName},{trace})")
    with open(csvFileName) as csvFile, db.bid.Table() as table:
        table.db=dbName
        table.trace=trace
        formRows = csv.DictReader(csvFile)
        for formRow in formRows:
            dbRow={}
            for dbCol in ALIASES:
                for formCol in ALIASES[dbCol]:
                    if formCol in formRow:
                        dbRow[dbCol]=formRow[formCol]
            dbRow['timestamp']=googleform2isoformat.googleform2isoformat(dbRow['timestamp'])
            table.insertOrUpdate(dbRow)

if __name__ == '__main__':
    dbName = db.config.DEFAULT
    trace = False
    for arg in sys.argv[1:]:
        if arg == "--test":
            dbName = db.config.TEST
            continue
        if arg == "--trace":
            trace = True
            continue
        if arg == "--create":
            createTables(dbName,trace)
            continue
        if arg == "--drop":
            dropTables(dbName,trace)
            continue
        csvFormImport(arg,dbName,trace)
