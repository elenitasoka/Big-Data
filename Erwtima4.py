from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import csv

#connection
cloud_config= {
  'secure_connect_bundle': 'C:\\Users\\eleni\\Desktop\\bigData23\\secure-connect-bigdataproject.zip'
}
auth_provider = PlainTextAuthProvider('haHsCWxAlEaIihYjxtevKCpT', 'gal0SGIKaWU+peZC1P+ouKa,fgZgkK_fmm8JO7y5KckvnRFaZoeo9OHclvSolNKf1323zaEB+uE5vj8TDTjlxOynjp.UaT5Z+NJv6N1b,TWF_RC3flhpqQPhls31f3tZ')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect('bigdata')

#create table
#session.execute ("""CREATE TABLE IF NOT EXISTS erwtima4 (
 #   name text,
  #  id int,
   # tag text,
    #submitted date,
    #PRIMARY KEY ((tag), submitted,id)
#) WITH CLUSTERING ORDER BY (submitted DESC, id ASC);
#""")

result=session.execute ("SELECT name, tag, submitted FROM erwtima4 WHERE tag='superbowl' ORDER BY submitted DESC ALLOW FILTERING;")

for results in result:
  print(results[0], results[1], results[2])


# Close the connection
cluster.shutdown()

