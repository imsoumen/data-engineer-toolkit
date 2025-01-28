from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when, lag, lead, coalesce, lit, sum, avg, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


spark = SparkSession.builder.appName("Problem Solving").getOrCreate()

# Sample data
# Pyspark DDL

products_schema = StructType([\
        StructField("product_id", IntegerType(), nullable=False),\
        StructField("product_name", StringType(), nullable=False),\
        StructField("category", StringType(), nullable=False)\
    ])

sales_schema = StructType([\
    StructField("product_id", IntegerType(), nullable=False),\
    StructField("year", IntegerType(), nullable=False),\
    StructField("total_sales_revenue", DoubleType(), nullable=False)\
])

products_data = [\
        (1, 'Laptops', 'Electronics'),\
        (2, 'Jeans', 'Clothing'),\
        (3, 'Chairs', 'Home Appliances')\
    ]

products_df = spark.createDataFrame(data=products_data, schema=products_schema)

sales_data = [\
        (1, 2019, 1000.00),\
        (1, 2020, 1200.00),\
        (1, 2021, 1100.00),\
        (2, 2019, 500.00),\
        (2, 2020, 600.00),\
        (2, 2021, 900.00),\
        (3, 2019, 300.00),\
        (3, 2020, 450.00),\
        (3, 2021, 400.00)\
    ]
    
sales_df = spark.createDataFrame(data=sales_data, schema=sales_schema)

sales_data_df = sales_df.withColumn('previous_sales',
                                    coalesce(lag('total_sales_revenue').over(Window.partitionBy('product_id').orderBy('year')),lit(0))
                                    )\
                        .withColumn('diff',
                                    col('total_sales_revenue') - col('previous_sales')
                                    )
joined_df = sales_data_df.join(products_df, sales_data_df.product_id == products_df.product_id, how='inner')\
                        .select(products_df.product_id,\
                                products_df.product_name,\
                                products_df.category,\
                                sales_data_df.diff\
                                )

grouped_df = joined_df.groupBy("product_id","product_name","category").agg(min("diff").alias("min_diff"))

grouped_df.filter(col("min_diff") >= 0).drop("min_diff").show()