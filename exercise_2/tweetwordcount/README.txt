README

This is a streamparse appplication to parse the twitter Streaming API and record the occurrences and counts of individial words in the twitter steam into a Postgres db.

Pre-requisites:
- Streamparse and all its dependencies must be installed.
- Python 2.7 is required to run the application.
- the python libraries matplotlib and numpy are required to run top20.py (the top 20 words bar char)
- Postgres must installed and running

Steps to run:-

1. clone the repository to the AWS instance
2. navigate to the tweetwordcount folder
3. Create the postgres db and table: 
    python create create_dbandtable.py
   NOTE: This will create the database Tcount and table Tweetwordcount inside. The db and table names are case-sensitive.
4. Run the streamparse application:
    sparse run
   When the application runs, you will see a stream of words which are also being written to the postgres db. Stop the application anytime using Ctrl-C
5. Run the python script finalwords.py to see counts for a single word or all words (leave out the parameter). Run histogram.py to see all words with a specific range of occurrences.
6. To create a bar chart called plot.png, run top20.py
   

    
