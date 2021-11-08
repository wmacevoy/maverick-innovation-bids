import sqlite3
import config
import filters
import generic

TABLE_NAME="sample"
COL_ID='id'
COL_TIMESTAMP='timestamp'
COL_COVID_PPM='covidPPM'
COL_LOCATION_ID='locationID'
COL_COLLECTOR_ID='collectorID'

def drop_sample_table(db=config.DB_NAME):
    generic.drop_table(TABLE_NAME,db)

def create_sample_table(db=config.DB_NAME):
    print("create_sample_table()")
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = f"""
    create table if not exists {TABLE_NAME} (
        {COL_ID} integer primary key,
        {COL_TIMESTAMP} text,
        {COL_COVID_PPM} real,
        {COL_LOCATION_ID} integer,
        {COL_COLLECTOR_ID} integer
    )
    """
    cursor.execute(sql)
    connection.commit()
    connection.close()


def insert_sample(values,db=config.DB_NAME):
    print("insert_sample()")
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = f"""
      insert into sample ({COL_TIMESTAMP}, {COL_COVID_PPM}, {COL_COLLECTOR_ID}, {COL_LOCATION_ID})
      values (:{COL_TIMESTAMP}, :{COL_COVID_PPM}, :{COL_COLLECTOR_ID}, :{COL_LOCATION_ID})
      """
    params = {
        COL_TIMESTAMP: filters.dbString(values[COL_TIMESTAMP],True),
        COL_COVID_PPM: filters.dbReal(values[COL_COVID_PPM],True), 
        COL_LOCATION_ID: filters.dbInteger(values[COL_LOCATION_ID],True), 
        COL_COLLECTOR_ID: filters.dbInteger(values[COL_COLLECTOR_ID],True) }
    cursor.execute(sql,params)
    connection.commit()
    connection.close()
    return cursor.lastrowid

def select_min_and_max_by_location_name(location_name, db=config.DB_NAME):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql=f"""
    select min({TABLE_NAME}.{COL_COVID_PPM},max({TABLE_NAME}.{COL_COVID_PPM}) 
      from {TABLE_NAME} inner join location on ({TABLE_NAME}.{COL_LOCATION_ID} = location.id) 
      where (location.name = :location_name)
    """
    # select min(sample.covidPPM),max(sample.covidPPM) from sample inner join location on (sample.locationId = location.id) where (location.name = 'uc');
    params = {'location_name': filters.dbString(location_name)}
    cursor.execute(sql,params)
    response=cursor.fetchall()
    connection.close()
    if response != None:
        min=response[0][0]
        max=response[0][1]
        print((min,max))
        return (min,max)
    else:
        print(f"no records at location {location_name}.")
        return (None,None)

def select_rows_by_location_and_sample_range(location_name, a, b, db=config.DB_NAME):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql=f"""
    select {TABLE_NAME}.{COL_ID} from {TABLE_NAME} inner join location on (sample.{COL_LOCATION_ID} = location.id) 
        where (location.name = :location_name and {TABLE_NAME}.{COL_COVID_PPM} >= :a and {TABLE_NAME}.{COL_COVID_PPM} <= :b)
    """
    params = {'location_name': filters.dbString(location_name), 
                'a': filters.dbReal(a), 'b': filters.dbReal(b)}
    cursor.execute(sql,params)
    response=cursor.fetchall()
    connection.close()
    print (response)
    return response

def select_rows_by_location_and_timestamp(location_name, timestamp, db=config.DB_NAME):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql=f"""
    select {TABLE_NAME}.{COL_ID} from {TABLE_NAME} inner join location on (sample.{COL_LOCATION_ID} = location.id) 
        where (location.name = :location_name and {TABLE_NAME}.{COL_TIMESTAMP} = :{COL_TIMESTAMP})
    """
    params = {'location_name': filters.dbString(location_name), 
                COL_TIMESTAMP : filters.dbString(timestamp)}
    cursor.execute(sql,params)
    response=cursor.fetchall()
    connection.close()
    print (response)
    return response

def select_sample_id_by_location_name(location_name, db=config.DB_NAME):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql=f"""
    select sample.id, location.id from sample inner join location on (sample.locationID = location.id) where (location.name = :location_name)
    """
    params = {'location_name': filters.dbString(location_name)}
    cursor.execute(sql,params)
    response=cursor.fetchall()
    connection.close()
    if response != None:
        print(response)
        return response
    else:
        print(f"no records at location {location_name}.")
        return None

def select_sample_by_id(id,db=config.DB_NAME):
    print("select_sample_by_id()")
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = f"""
      select {COL_ID},{COL_TIMESTAMP}, {COL_COVID_PPM}, {COL_COLLECTOR_ID}, {COL_LOCATION_ID} from {TABLE_NAME}
      where ({COL_ID} = :{COL_ID})
      """
    params = {COL_ID: filters.dbInteger(id)}
    cursor.execute(sql,params)
    response=cursor.fetchone()
    connection.close()
    if response != None:
        return {
            COL_ID : response[0],
            COL_TIMESTAMP: response[1],
            COL_COVID_PPM: response[2],
            COL_COLLECTOR_ID: response[3],
            COL_LOCATION_ID: response[4]
        }
    else:
        return None

def test_sample():
    db=config.DB_TEST_NAME
    drop_sample_table(db)
    create_sample_table(db)
    id1=insert_sample({
        COL_TIMESTAMP: '2021-11-02T16:16:14',
        COL_COVID_PPM: 3.14, 
        COL_LOCATION_ID: 1, 
        COL_COLLECTOR_ID: 2},
        db)
    id2=insert_sample({
        COL_TIMESTAMP: '2023-11-02T16:16:14',
        COL_COVID_PPM: 5.00, 
        COL_LOCATION_ID: 13, 
        COL_COLLECTOR_ID: 22},
        db)
    row1=select_sample_by_id(id1,db)
    row2=select_sample_by_id(id2,db)
    rowNone=select_sample_by_id(32984057,db)
    if rowNone != None:
        raise ValueError('not none')
    if row1[COL_ID] != id1:
        raise ValueError('id1 id wrong:' + str(row1[COL_ID]))
    if row1[COL_COVID_PPM] != 3.14:
        raise ValueError('id1 sample wrong.')
    if row2[COL_LOCATION_ID] != 13:
        raise ValueError('id2 location wrong.')
    if row2[COL_COLLECTOR_ID] != 22:
        raise ValueError('id2 collector wrong.')
    if row2[COL_TIMESTAMP] != '2023-11-02T16:16:14':
        raise ValueError('id2 collector wrong.')
