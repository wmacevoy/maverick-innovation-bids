import sqlite3
import sys,csv
import db.config
from db.schema.bid import *

import db.user
import db.item

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
                {COL_TIMESTAMP} text,
                {COL_USER_ID} integer,
                {COL_ITEM_ID} integer,
                {COL_OFFER} float)
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

    def selectIdsByTimestampUserItem(self,timestamp,userId,itemId):
        what = COL_ID
        where = f"""{COL_TIMESTAMP}=:{COL_TIMESTAMP} and {COL_USER_ID}=:{COL_USER_ID} and {COL_ITEM_ID}=:{COL_ITEM_ID}"""
        params = {
            COL_TIMESTAMP: FILTERS[COL_TIMESTAMP](timestamp),
            COL_USER_ID: FILTERS[COL_USER_ID](userId),
            COL_ITEM_ID: FILTERS[COL_ITEM_ID](itemId)}
            
        results=self.select(what,where,params)
        return results

    def insertOrUpdate(self,params):
        if self._trace: print(f"insert_or_update({params})")
        params=params.copy()
        userId = params[COL_USER_ID] if COL_USER_ID in params else None
        if userId == None:
            userTable=db.user.Table()
            userTable.connection=self.connection
            userParams = {}
            if 'user.name' in params:
                userParams[db.user.COL_NAME]=params['user.name']
            if 'user.email' in params:
                userParams[db.user.COL_EMAIL]=params['user.email']
            userId=userTable.insertOrUpdate(userParams)
            params[COL_USER_ID]=userId

        itemId = params[COL_ITEM_ID] if COL_ITEM_ID in params else None
        if itemId == None:
            itemTable=db.item.Table()
            itemTable.connection=self.connection
            itemParams={}
            if 'item.name' in params:
                itemParams[db.item.COL_NAME]=params['item.name']
            itemId=itemTable.insertOrUpdate(itemParams)
            params[COL_ITEM_ID]=itemId
        
        id = params[COL_ID] if COL_ID in params else None
        if id == None:
            timestamp = params[COL_TIMESTAMP] if COL_TIMESTAMP in params else None            
            results=self.selectIdsByTimestampUserItem(timestamp,userId,itemId)
            if len(results) == 1:
                id = results[0][0]
            elif len(results) > 1:
                raise ValueError("ambiguous record")

        if id == None:
            id = self.insert(params)
        else:
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
