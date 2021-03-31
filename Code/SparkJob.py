from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AirlinesDataset").getOrCreate()

Airdata = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("s3://iacsd/2017.csv")

spark.conf.set("spark.debug.maxToStringFields", 10000)
Airdata.createOrReplaceTempView("Airdata")

sqlproperties = {"user":"admin", "password":"admin123", "driver":"com.mysql.jdbc.Driver"}

distance=spark.sql("select op_carrier,sum(distance) from Airdata group by op_carrier")
distance.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="distance",properties=sqlproperties)

delaynum=spark.sql("select count(op_carrier_fl_num),op_carrier from Airdata where arr_delay>0 group by op_carrier")
delaynum.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="delay_count",properties=sqlproperties)

nondelay=spark.sql("select count(op_carrier_fl_num),op_carrier from Airdata where arr_delay<=0 group by op_carrier")
nondelay.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="nondelay_count",properties=sqlproperties)

cancelled=spark.sql("select count(op_carrier_fl_num),op_carrier from Airdata where cancelled=1 group by op_carrier")
cancelled.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="cancle_count",properties=sqlproperties)

diverted=spark.sql("select count(op_carrier_fl_num),op_carrier from Airdata where diverted=1 group by op_carrier")
diverted.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="divert_count",properties=sqlproperties)

flights_count=spark.sql("select op_carrier,count(op_carrier_fl_num) from Airdata group by op_carrier")
flights_count.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="flights_count",properties=sqlproperties)

origin=spark.sql("select origin,count(origin) as source from Airdata group by origin order by source desc limit 5")
origin.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="origin",properties=sqlproperties)

dest=spark.sql("select dest,count(dest) as destn from Airdata group by dest order by destn desc limit 5")
dest.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="destination",properties=sqlproperties)

avg_delay=spark.sql("select op_carrier,avg(arr_delay) as avg_arr_delay from Airdata group by op_carrier order by avg_arr_delay")
avg_delay.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="avg_delay",properties=sqlproperties)

delay=spark.sql("select op_carrier,sum(arr_delay) as act_arr_delay from Airdata group by op_carrier order by act_arr_delay desc")
delay.write.jdbc(url="jdbc:mysql://database-1.cdzj5btssyqo.ap-southeast-1.rds.amazonaws.com/delaydb",table="total_delay",properties=sqlproperties)