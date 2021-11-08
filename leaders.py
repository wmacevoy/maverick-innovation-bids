#!/usr/bin/env python

import sys
import db.item
import db.user
import db.bid
from datetime import datetime

class Leaders:
    def __init__(self):
        self.item = db.item.Table()
        self.bid = db.bid.Table()
        self.user = db.user.Table()
        self._public = True
    
    @property
    def public(self):
        return self._public

    @public.setter
    def public(self, value):
        self._public = value
    
    @property
    def db(self):
        return self.bid.db

    @db.setter
    def db(self,value):
        self.bid.db=value
        self.item.db=value
        self.user.db=value

    @property
    def trace(self):
        return self.bid.trace

    @trace.setter
    def trace(self,value):
        self.item.trace = value
        self.user.trace = value
        self.bid.trace = value

    def leaders(self,itemId):
        details = self.item.row(itemId)

        minOffer = details[db.item.COL_MIN_OFFER]
        quantity = details[db.item.COL_QUANTITY]
    
        sql=f"""
select 
    bid.id,bid.timestamp,bid.offer,
    user.id,user.name,user.email,
    item.id,item.name
from bid 
    inner join user on (bid.userId = user.id) 
    inner join item on (bid.itemId = item.id) 
where (item.id = :itemId and bid.offer >= :minOffer) 
order by bid.offer desc,bid.timestamp desc limit :quantity
"""
        params = {
            'itemId':itemId,
            'minOffer':minOffer,
            'quantity':quantity}
        (cursor,exeResult)=self.bid.execute(sql,params)
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
    
    def items(self):
        result=self.item.select(db.item.COL_ID,"1=1",{})
        ids=[]
        for row in result:
            ids.append(row[0])
        return ids

    def title(self):
        utcnow=datetime.utcnow().isoformat()
        str = f"""# Bid Leaderboard as of {utcnow}
"""
        return str

    def nme(self,name):
        if not self._public: return name
        ucn=""
        ws=True
        for letter in name:
            if letter == ' ':
                ws = True
            elif ws:
                ucn += letter.upper()
                ws = False
        return ucn

    def eml(self,email):
        if not self._public: return email

        at = email.find('@')
        return email[0:min(3,at)]+"..@.."+email[max(at+3,len(email)-8):]

    def itemHeading(self,itemId):
        details=self.item.row(itemId)
        heading = f"""## Item #{details['id']} - {details['name']} ({details['minOffer']} minimum)

{details['description']}

"""
        return heading

    def itemLeaders(self,itemId):
        results=self.leaders(itemId)
        if len(results) == 0:
            return f"""No bids exceed minimum.

"""
        else:
            str = f"""|Rank|Time|Bid|Name|EMail|
|----|----|---|----|-----|
"""
        rank=0
        for result in results:
            rank += 1
            timestamp=result['bid.timestamp']
            offer=result['bid.offer']
            n=self.nme(result['user.name'])
            e=self.eml(result['user.email'])
            str += f"""|{rank}|{timestamp}|{offer}|{n}|{e}|
"""
        str += f"""
"""
        return str
    def markdown(self):
        str = ""
        str += self.title()
        for id in self.items():
            str += self.itemHeading(id)
            str += self.itemLeaders(id)
        return str

if __name__ == '__main__':
    leaders = Leaders()
    for arg in sys.argv[1:]:
        if arg == "--trace": 
            leaders.trace = True
            continue
        if arg == "--test":
            leaders.db = db.config.TEST
            continue

        if arg == "--private":
            leaders.public = False
            continue

        if arg == "--markdown":
            print(leaders.markdown())
