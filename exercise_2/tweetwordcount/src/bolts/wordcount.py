from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt

import psycopg2

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
        self.conn = psycopg2.connect(database="Tcount", user="postgres", password="", host="localhost", port="5432")
        self.cur = self.conn.cursor()
        self.cur.connection.set_isolation_level(0)

    def process(self, tup):
        word = tup.values[0]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        

        # Increment the local count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        self.cur.execute('SELECT 1 FROM "Tweetwordcount" WHERE word=%s', (word,))
        recs = self.cur.fetchall()
        if(len(recs) == 0):
            self.cur.execute('INSERT INTO "Tweetwordcount" (word,count) VALUES (%s, %s)', (word,self.counts[word]));
        else:
            self.cur.execute('UPDATE "Tweetwordcount" SET count=%s WHERE word=%s', (self.counts[word],word))
          
        
        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))

