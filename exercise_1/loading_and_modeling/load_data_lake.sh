#! /bin/bash

#login as w205 user; download an unzip the hospitals data file
su - w205
cd ~
mkdir exercise1
cd exercise1/
wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip
mv Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s\?content_type\=application%2Fzip\;\ charset\=binary  Hospital_Revised_Flatfiles.zip
unzip Hospital_Revised_Flatfiles.zip

#rename and copy the files to a new folder

mkdir selected

cp "Hospital General Information.csv" selected/hospitals.csv
cp  "Timely and Effective Care - Hospital.csv" selected/effective_care.csv
cp "Readmissions and Deaths - Hospital.csv" selected/readmissions.csv
cp "Measure Dates.csv" selected/measures.csv
cp hvbp_hcahps_05_28_2015.csv selected/surveys_responses.csv
 
cd selected/

#remove the header
tail -n +2 hospitals.csv > hospitals2.csv
tail -n +2 effective_care.csv > effective_care2.csv
tail -n +2 measures.csv > measures2.csv
tail -n +2 readmissions.csv > readmissions2.csv
tail -n +2 surveys_responses.csv > surveys_responses2.csv

#copy to HDFS
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put exercise1/selected/*2.csv /user/w205/hospital_compare

