import sys
import psycopg2
import bisect

#if there is an argument, use it as the word to lookup in db
word = ""
if len(sys.argv) > 1:
    word = sys.argv[1].lower()

#connect to db
conn = psycopg2.connect(database="Tcount", user="postgres", password="", host="localhost", port="5432")
cur = conn.cursor()
#cur.connection.set_isolation_level(0)

if word != "":
    #lookup word in table
    
    cur.execute('SELECT word, count from "Tweetwordcount" WHERE word=%s', (word,))
    rec = cur.fetchone()
    if rec:
        print "Total number of occurences of %s: %d" %(rec[0], rec[1])
else:
    #no word specified, output all words in sorted order
    
    sorted_recs = []
    cur.execute('SELECT word, count from "Tweetwordcount"')
    records = cur.fetchall()
    for rec in records:
        bisect.insort(sorted_recs, (rec[0], rec[1]))
    for t in sorted_recs:
        print "(%s , %d)" %(t[0], t[1])
