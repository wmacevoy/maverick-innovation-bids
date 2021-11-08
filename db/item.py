#!/usr/bin/env python

import sqlite3
import sys,csv
import db.config
from db.schema.item import *

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

    def drop(self):
        sql = f"drop table if exists {TABLE_NAME}"
        params = {}
        self.execute(sql,params)

    def create(self):
        self.execute(CREATE)

    def execute(self,sql,params={},commit=True):
        if (self._trace or True):
            print(f"execute sql={sql}; params={params}")
        connection=self.connection
        cursor=connection.cursor()
        result=cursor.execute(sql,params)
        if commit:
            self.commit()
        return (cursor,result)

    def insert(self,values):
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
        (cursor,result)=self.execute(sql,params)
        id = cursor.lastrowid
        if self._trace: print(f"id={id}")
        return id

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
        self.execute(sql,params)

    def select(self,what,where,params):
        sql = f"select {what} from {TABLE_NAME} where {where}"
        (cursor,result)=self.execute(sql,params)
        return cursor.fetchall()

    def row(self,id):
        params = {}
        selects = []

        for col in FILTERS:
            selects.append(col)
        sql = f"select {','.join(selects)} from {TABLE_NAME} where {COL_ID}=:{COL_ID}"
        params = {COL_ID: FILTERS[COL_ID](id)}
        (cursor,result)=self.execute(sql,params)
        all=cursor.fetchall()
        if len(all) == 1:
            index = 0
            ans = {}
            for col in FILTERS:
                ans[col]=all[0][index]
                index += 1
            return ans
        else:
            return None
            
    def selectIdsByName(self,name):
        return self.select(COL_ID,f"{COL_NAME}=:{COL_NAME}",{COL_NAME:FILTERS[COL_NAME](name)})

    def insertOrUpdate(self,params):
        if self._trace: print(f"insert_or_update({params})")

        id = params[COL_ID] if COL_ID in params else None
        if id == None:
            results=self.selectIdsByName(params[COL_NAME])
            if len(results) == 1:
                id = results[0][0]
            elif len(results) > 1:
                raise ValueError("ambiguous name {params[COL_NAME]}")

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
                
    def __enter__(self):
        return self

    def __exit__(self ,type, value, traceback):
        self.close()
