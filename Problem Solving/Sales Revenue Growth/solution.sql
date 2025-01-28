select * from sales;

select * from products;


with sales_data as
(   select
        *,
        lag(total_sales_revenue) over (partition by product_id order by year) as previous_sales,
        total_sales_revenue - coalesce(lag(total_sales_revenue) over (partition by product_id order by year),0) as diff
    from sales
    order by product_id,year
)
select
    s.product_id,
    p.product_name,
    p.category
from sales_data s
inner join products p
on s.product_id = p.product_id
group by s.product_id, p.product_name, p.category
HAVING MIN(s.diff) >= 0;