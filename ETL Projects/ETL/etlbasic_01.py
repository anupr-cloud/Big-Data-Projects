##import required libraries
import pyspark

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath',"C:/Users/ANOOP/Downloads/postgresql-42.6.0.jar") \
   .getOrCreate()


##read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/movies") \
   .option("dbtable", "movie") \
   .option("user", "<username>") \
   .option("password", "<password>") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the movies_df
print(movies_df.show())

