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

# print(products_df)
# print(sales_df)

sales_df['previous_sales'] = sales_df.groupby('product_id')['total_sales_revenue'].shift(1).fillna(0)

sales_df['diff'] = sales_df['total_sales_revenue'] - sales_df['previous_sales']

joined_df = pd.merge(sales_df, products_df, on='product_id', how='inner')

grouped_df = joined_df.groupby(['product_id', 'product_name', 'category'])['diff'].min().reset_index()

result_df = grouped_df[grouped_df['diff'] >= 0].drop('diff', axis=1) 

print(result_df)
