#!/usr/bin/env python

import sys,csv
import db.config
import db.bid
import googleform2isoformat

ALIASES = {
    'timestamp' : ['timestamp','Timestamp'],
    'user.name' : ['user.name','What is your name?'],
    'user.email' : ['user.email','What is your Colorado Mesa University email address?'],
    'item.name' : ['item.name','What are you bidding for (one item per submission)'],
    'offer' : ['What is your offer for this item?']
}

def csvFormImport(csvFileName,dbName=db.config.DEFAULT,trace=False):
    dbBidTable=db.bid.Table()
    dbBidTable.db = dbName
    dbBidTable.trace = trace
    dbBidTable.create()
    with open(csvFileName) as csvFile:
        formRows = csv.DictReader(csvFile)
        for formRow in formRows:
            dbRow={}
            for dbCol in ALIASES:
                for formCol in ALIASES[dbCol]:
                    if formCol in formRow:
                        dbRow[dbCol]=formRow[formCol]
            dbRow['timestamp']=googleform2isoformat.googleform2isoformat(dbRow['timestamp'])
            dbBidTable.insertOrUpdate(dbRow)

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
        csvFormImport(arg,dbName,trace)
