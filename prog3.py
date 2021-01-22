#couvert clement

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number
from pyspark.sql.functions import max, min
from pyspark.sql.functions import mean
from pyspark.sql.functions import corr
from pyspark.sql.functions import year
from pyspark.sql.functions import month

#1
spark = SparkSession.builder.appName('walmart_stock').getOrCreate()
sc = spark.sparkContext

#2
df=spark.read.csv("walmart_stock.csv",header=True)
df.createOrReplaceTempView('Table')
spark.sql("""SELECT * FROM Table""").show()

#3
print(df.columns)

#4
df.printSchema()

#5
df_ratio = df.withColumn('HV_Ratio', df['High']/df['Volume']).select(['HV_Ratio'])
df_ratio.show()
spark.sql("""SELECT High/Volume as HV_Ratio FROM Table""").show()

#6
print(df.orderBy(df['High'].desc()).select(['Date']).head(1)[0]['Date'])
spark.sql("""SELECT ROUND(MAX(High), 2 ) as Prix FROM Table""").show()

#7
df.select(mean('Close')).show()
spark.sql("""SELECT ROUND(MEAN(Close), 2) as Moyenne FROM Table""").show()

#8
df.select(max('Volume'),min('Volume')).show()
spark.sql("""SELECT MAX(Volume) as MAX, MIN(Volume) as MIN FROM Table""").show()

#9
days = df.filter(df['Close'] < 60).count()
print("Il y avait", days ,"jours durant lesquels la valeur Close était inférieur à 60.")
spark.sql("""SELECT COUNT(Date) as Jours FROM Table WHERE Close < 60""").show()

#10
percentage = df.filter('High > 80').count() * 100/df.count()
print("Il y avait", round(percentage, 2), "% de temps durant lequel la valeur High était supérieur à 80 dollars.")
spark.sql("""SELECT ROUND((SELECT COUNT(High) FROM Table WHERE High > 80)*100/COUNT(High), 2) as Max_High FROM Table""").show()

#11
spark.sql("""SELECT YEAR(Date) as Year, ROUND(MAX(High), 2) as Max_High FROM Table GROUP BY Year ORDER BY Year""").show()

#12
spark.sql("""SELECT MONTH(Date) as Month, ROUND(AVG(Close), 2) as AVG_Close FROM Table GROUP BY MONTH(Date) ORDER BY MONTH(Date)""").show()
