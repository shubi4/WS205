import sys
import psycopg2
import bisect

word = ""
if len(sys.argv) < 3:
    print "Too few arguments: usage: %s count_low count_high" %(sys.argv[0])
    sys.exit()
    
count1 = int(sys.argv[1])
count2 = int(sys.argv[2])

#connect to db
conn = psycopg2.connect(database="Tcount", user="postgres", password="", host="localhost", port="5432")
cur = conn.cursor()

cur.execute('SELECT word, count from "Tweetwordcount"')

#output records that are between the two specified counts
records = cur.fetchall()
for rec in records:
    if rec[1] >= count1 and rec[1] <= count2:
        print "%s : %d" %(rec[0], rec[1])


