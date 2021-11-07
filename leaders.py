#!/usr/bin/env python

import db.item
import db.user
import db.bid
from datetime import datetime

dbItemTable=db.item.Table()
dbItemTable.trace = False
dbBidTable=db.bid.Table()


def itemDetails(itemId):
    results=dbItemTable.select(f"{db.item.COL_NAME},{db.item.COL_DESCRIPTION},{db.item.COL_MIN_OFFER},{db.item.COL_QUANTITY}",f"{db.item.COL_ID}=:{db.item.COL_ID}",{db.item.COL_ID: itemId})
    (itemName,itemDescription,itemMinOffer,itemQuantity)=results[0]
    return {'item.name':itemName,'item.description':itemDescription,'item.minOffer':itemMinOffer,'item.quantity':itemQuantity}

def leaders(itemId):
    results=dbItemTable.select(f"{db.item.COL_MIN_OFFER},{db.item.COL_QUANTITY}",f"{db.item.COL_ID}=:{db.item.COL_ID}",{db.item.COL_ID: itemId})
    (minOffer,quantity)=results[0]
    
    sql=f"select bid.id,bid.timestamp,bid.offer,user.id,user.name,user.email,item.id,item.name from bid inner join user on (bid.userId = user.id) inner join item on (bid.itemId = item.id) where (item.id = :itemId and bid.offer >= :minOffer) order by bid.offer desc,bid.timestamp desc limit :quantity"
    params = {'itemId':itemId,'minOffer':minOffer,'quantity':quantity}
    connection = dbBidTable.connection
    cursor = connection.cursor()
    cursor.execute(sql,params)
    results = cursor.fetchall()
    dictResults=[]
    for result in results:
        (bidId,bidTimestamp,bidOffer,userId,userName,userEMail,itemId,itemName)=result
        dictResults.append({
            'bid.id': bidId, 'bid.timestamp':bidTimestamp, 'bid.offer': bidOffer,
            'user.id':userId,'user.name':userName,'user.email':userEMail,
            'item.id':itemId,'item.name':itemName
        })
    return dictResults
    


items=dbItemTable.select(f"{db.item.COL_ID}","1=1",{})
utcnow=datetime.utcnow().isoformat()

print(f"# Bid Leaderboard")
print()
print(f"Updated {utcnow} UTC.")
print()

for (itemId,) in items:
    details=itemDetails(itemId)
    print(f"## Leaders for {details['item.name']}")
    print()
    print(f"{details['item.description']}")
    print()
    print(f"Minimum bid: {details['item.minOffer']}")
    print()
    results=leaders(itemId)
    if len(results) == 0:
        print(f"No bids exceed minimum.")
    else:
        print(f"|Rank|Time|Bid|Name|EMail|")
        print(f"|----|----|---|----|-----|")        
        rank=0
        for result in results:
            rank += 1
            print(f"|{rank}|{result['bid.timestamp']}|{result['bid.offer']}|{result['user.name']}|{result['user.email']}|")
    print()

