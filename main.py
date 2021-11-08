#!/usr/bin/env python

import db.item
import db.user
import db.bid

dbItemTable=db.item.Table()
dbItemTable.trace=True
dbItemTable.csvImport('db/item.csv')

dbUserTable=db.user.Table()
dbUserTable.trace=True
dbUserTable.csvImport('db/user.csv')

dbBidTable=db.bid.Table()
dbBidTable.trace=True
dbBidTable.csvImport('db/bid.csv')

itemIds=dbItemTable.select(db.item.COL_ID,"1=1",{})

for itemId in itemIds:
    itemId=itemId[0]
    results=dbItemTable.select(f"{db.item.COL_MIN_OFFER},{db.item.COL_QUANTITY}",f"{db.item.COL_ID}=:{db.item.COL_ID}",{db.item.COL_ID: itemId})
    (minOffer,quantity)=results[0]
    sql=f"select bid.id,bid.offer,user.id,user.name,user.email,item.id,item.name from bid inner join user on (bid.userId = user.id) inner join item on (bid.itemId = item.id) where (item.id = :itemId and bid.offer >= :minOffer) order by bid.offer desc,bid.timestamp desc limit :quantity"
    params = {'itemId':itemId,'minOffer':minOffer,'quantity':quantity}
    connection = dbBidTable.connection
    cursor = connection.cursor()
    cursor.execute(sql,params)
    result = cursor.fetchall()
    print(result)


