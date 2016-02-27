
#save the provider id and name
sp = sc.textFile("hdfs:///user/w205/hospital_compare/hospitals/")
hosp_trimmed = hosp.map(lambda r: r.split(",")).map(lambda r: (r[0].replace('"', ''), (r[1].replace('"', ''), r[4].replace('"', ''))))
hosp_trimmed.saveAsTextFile("hdfs:///user/w205/hospital_compare/hosp_trimmed2.csv")

