import petl as etl, psycopg2 as pg

#Connecting to the local PostgresSQL Server
dbConnect = {'Northwind':"dbname=Northwind user=postgres password=P@SS host=127.0.0.1",
'postgres':"dbname=postgres user=postgres password=P@SS host=127.0.0.1"}

sourceConn = pg.connect(dbConnect['Northwind'])
targetConn = pg.connect(dbConnect['postgres'])

#Creating the cursors
sourceCur = sourceConn.cursor()
targetCur = targetConn.cursor()

#Getting the table from the source database
sourceCur.execute("""SELECT DISTINCT table_name FROM information_schema.columns WHERE table_name = 'employees'""")
sourceTables = sourceCur.fetchall()

#Dropping the table if exits and loading it 
for t in sourceTables:
    targetCur.execute("drop table if exists %s" % (t[0]))
    sourceDest = etl.fromdb(sourceConn,"select * from %s" % (t[0]))
    etl.todb(sourceDest,targetConn,t[0],create=True,sample=1000)
