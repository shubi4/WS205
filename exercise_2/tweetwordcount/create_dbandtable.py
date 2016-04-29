import psycopg2

conn = psycopg2.connect(user="postgres", password="", host="localhost", port="5432")
cur = conn.cursor()
cur.connection.set_isolation_level(0)

cur.execute('CREATE DATABASE "Tcount"')
print "created database Tcount"

conn.commit()
cur.close()
conn.close()

conn = psycopg2.connect(database="Tcount", user="postgres", password="", host="localhost", port="5432")
cur = conn.cursor()
cur.connection.set_isolation_level(0)

cur.execute('''CREATE TABLE "Tweetwordcount" 
   (word TEXT PRIMARY KEY     NOT NULL,
   count INT     NOT NULL);''')
print "created table Tweetwordcount"

conn.commit()
cur.close()
conn.close()

