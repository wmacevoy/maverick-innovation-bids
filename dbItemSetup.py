#!/usr/bin/env python

import sys,csv
import db.config
import db.item

if __name__ == '__main__':
    table=db.item.Table()
    imported=False
    
    for arg in sys.argv[1:]:
        if arg == "--test":
            table.db = db.config.TEST
            continue
        if arg == "--trace":
            table.trace = True
            continue
        imported=True
        table.csvImport(arg)
    if not imported:
        imported=True
        table.csvImport('db/item.csv')
