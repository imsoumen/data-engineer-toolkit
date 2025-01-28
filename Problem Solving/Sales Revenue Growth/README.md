# <ins>**Problem Description:**</ins>

Link: 

Write a solution to find the products whose total sales revenue has increased every year. Return product_id, product_name and category

<ins>**SQL DDL:**</ins>

CREATE TABLE products (\
  product_id INT PRIMARY KEY,\
  product_name VARCHAR(50),\
  category VARCHAR(50)\
);

INSERT INTO products (product_id, product_name, category) VALUES\
  (1, 'Laptops', 'Electronics'),\
  (2, 'Jeans', 'Clothing'),\
  (3, 'Chairs', 'Home Appliances');


CREATE TABLE sales (\
  product_id INT,\
  year INT,\
  total_sales_revenue DECIMAL(10, 2),\
  PRIMARY KEY (product_id, year),\
  FOREIGN KEY (product_id) REFERENCES products(product_id)\
);

INSERT INTO sales (product_id, year, total_sales_revenue) VALUES\
  (1, 2019, 1000.00),\
  (1, 2020, 1200.00),\
  (1, 2021, 1100.00),\
  (2, 2019, 500.00),\
  (2, 2020, 600.00),\
  (2, 2021, 900.00),\
  (3, 2019, 300.00),\
  (3, 2020, 450.00),\
  (3, 2021, 400.00);



<ins>**PySpark DDL:**</ins>

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

<ins>**Python DDL:**</ins>

import pandas as pd

products_data = [\
    (1, 'Laptops', 'Electronics'),\
    (2, 'Jeans', 'Clothing'),\
    (3, 'Chairs', 'Home Appliances')\
]

products_df = pd.DataFrame(products_data, columns=['product_id', 'product_name', 'category'])

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

sales_df = pd.DataFrame(sales_data, columns=['product_id', 'year', 'total_sales_revenue'])
