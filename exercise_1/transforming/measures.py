
#save measure id and name
measures = sc.textFile("hdfs:///user/w205/hospital_compare/measures/")
meas_trimmed = measures.map(lambda r: r.split(",")).map(lambda r: (r[1].replace('"', ''), r[0].replace('"', '')))
meas_trimmed.saveAsTextFile("hdfs:///user/w205/hospital_compare/meas_trimmed.csv")

