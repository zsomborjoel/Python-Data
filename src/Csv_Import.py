import csv
import psycopg2 as pg

#Connecting to PostgreSQL
dbConnect = {'postgres':"dbname=postgres user=postgres password=P@ss host=127.0.0.1"}
conn = pg.connect(dbConnect['postgres'])

#Opening cursor
cur = conn.cursor()

#Opeaning Csv file and inserting into the database
with open('language.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        print(row)
        cur.execute("""INSERT INTO public.language(language_id, name, last_update)
                        VALUES (%s, %s, %s);""", row)

conn.commit()
