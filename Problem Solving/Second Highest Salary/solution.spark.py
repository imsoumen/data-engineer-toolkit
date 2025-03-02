from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("SwapSeats").getOrCreate()

# Sample data
data = [[1, 100], 
        [2, 200], 
        [3, 300]]

df = spark.createDataFrame(data, ["id", "salary"])

ranked_df = df.withColumn("rank", row_number().over(Window.orderBy(desc("salary"))))

df_output = ranked_df.filter(col("rank") == 2)

df_output.select("id", "salary").show()