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
#session.execute ("""CREATE TABLE IF NOT EXISTS best_rating (
 #   recipe_id int,
   # date date,
    #rating int,
    #name text,
    #PRIMARY KEY ((recipe_id), date)
#) WITH CLUSTERING ORDER BY (date ASC);
#""")

result=session.execute ("SELECT name, rating, date FROM best_rating WHERE date>'2012-01-01' AND date<'2012-05-31' LIMIT 30 ALLOW FILTERING;")

for results in result:
  print(results[0], results[1], results[2])

# Close the connection
cluster.shutdown()

