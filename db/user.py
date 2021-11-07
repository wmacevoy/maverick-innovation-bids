#!/usr/bin/env python

import sqlite3
import csv,csv
import db.config
from db.schema.user import *

class Table:
    def __init__(self):
        self._db = None
        self._connection = None
        self._cursor = None
        self._trace = False

    def commit(self):
        if self._connection != None:
            self._connection.commit()

    def close(self):
        self.commit()
        if self._connection != None:
            self._connection.close()
            self._connection = None

    @property
    def trace(self):
        return self._trace

    @trace.setter
    def trace(self,value):
        self._trace = value
    

    @property
    def db(self):
        if self._db == None:
            self._db = db.config.DEFAULT
            if (self._trace): print(f"{TABLE_NAME}: default db")
        return self._db

    @db.setter
    def db(self,value):
        if (self._trace): print(f"{TABLE_NAME}: set db as {value}")        
        self.close()
        self._db = str(value)

    @property
    def connection(self):        
        if self._connection == None:
            if (self._trace): print(f"{TABLE_NAME}: new connect")
            self._connection = sqlite3.connect(self.db)
        return self._connection

    @connection.setter
    def connection(self,value):
        self.close()
        self._connection = value

    def execute(self,sql,params=()):
        connection=self.connection

    def drop(self):
        cursor = self.connection.cursor()
        sql = f"drop table if exists {TABLE_NAME}"
        params = {}
        cursor.execute(sql,params)
        self.commit()

    def create(self):
        cursor = self.connection.cursor()
        sql = f"""
            create table if not exists {TABLE_NAME} (
                {COL_ID} integer primary key,
                {COL_NAME} text,
                {COL_EMAIL} text)
        """
        params={}
        cursor.execute(sql,params)
        self.commit()

    def insert(self,values):
        cursor = self.connection.cursor()
        cols = FILTERS.keys()
        noIdCols=[]
        for col in cols:
            if col != COL_ID:
                noIdCols.append(col)

        noIdColsList = ",".join(noIdCols)
        noIdColsColonList = ",:".join(noIdCols)
        sql = f"""
            insert into {TABLE_NAME}
                ({noIdColsList})
                values
                (:{noIdColsColonList})
        """
        params = {}
        for col in noIdCols:
            params[col]=FILTERS[col](values[col])
        if self._trace: print(f"sql: {sql} params: {params}")
        cursor.execute(sql,params)
        self.commit()
        return cursor.lastrowid

    def update(self,values):
        params = {}
        sets = []

        for col in FILTERS:
            filter=FILTERS[col]
            if col == COL_ID:
                id=filter(values[col])
                params[col]=id
            elif col in values:
                sets.append(f"""{col}=:{col}""")
                params[col]=filter(values[col])

        setstr = ",".join(sets)
        sql = f"update {TABLE_NAME} set {setstr} where {COL_ID}=:{COL_ID}"
        if self._trace: print(f"sql: {sql} params: {params}")
 
        cursor = self.connection.cursor()
        cursor.execute(sql,params)
        self.commit()

    def select(self,what,where,params):
        sql = f"select {what} from {TABLE_NAME} where {where}"
        if self._trace: print(f"sql: {sql} params: {params}")
        cursor = self.connection.cursor()
        cursor.execute(sql,params)
        return cursor.fetchall()

    def selectIdsByEMail(self,name):
        return self.select(COL_ID,f"{COL_EMAIL}=:{COL_EMAIL}",{COL_EMAIL:FILTERS[COL_EMAIL](name)})

    def insertOrUpdate(self,params):
        if self._trace: print(f"insert_or_update({params})")

        id = params[COL_ID] if COL_ID in params else None
        if id == None:
            results=self.selectIdsByEMail(params[COL_EMAIL])
            if len(results) == 1:
                id = results[0][0]
            elif len(results) > 1:
                raise ValueError("ambiguous email {params[COL_EMAIL]}")

        if id == None:
            id = self.insert(params)
        else:
            if not COL_ID in params:
                params = params.copy()
                params[COL_ID] = id
            self.update(params)
        return id

    def csvImport(self,csvFileName):
        if self._trace: print(f"csvImport({csvFileName})")
        self.create()
        with open(csvFileName) as csvFile:
            rows = csv.DictReader(csvFile)
            for row in rows:
                self.insertOrUpdate(row)

if __name__ == '__main__':
    table=Table()
    for arg in sys.argv[1:]:
        if arg == "--test":
            table.db = db.config.TEST
            continue
        if arg == "--trace":
            table.trace = True
            continue
        table.csvImport(arg)
