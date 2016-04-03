# extract provider id, measure id, score
ef = sc.textFile("hdfs:///user/w205/hospital_compare/effective_care/")
ef_trimmed = ef.map(lambda r: r.split(",")).map(lambda r: (r[0].replace('"', ''), r[9].replace('"', ''),  r[11].replace('"', '')))
ef_trimmed.saveAsTextFile("hdfs:///user/w205/hospital_compare/effective_trimmed.csv")

