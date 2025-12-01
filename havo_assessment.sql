-- Monthly revenue trend

with monthly_revenue as (
select 
  strftime('%Y-%m-01',created_at) as month , 
  lower(payment_status) as payment_status ,
  round(sum(Total),2) as  revenue
from transactions_df_transformed
where lower(payment_status) = 'paid'  and strftime('%Y-01-01',created_at) <= '2025-12-01'
group by strftime('%Y-%m-01',created_at),  lower(payment_status)
  )
  
  ,stats as (
  select 
  round(avg(revenue),2) as mean,
  sqrt(AVG((revenue - (SELECT AVG(revenue) FROM monthly_revenue)) * 
                (revenue - (SELECT AVG(revenue) FROM monthly_revenue)))) AS std_rev
  from monthly_revenue
  where lower(payment_status) = 'paid'
    )
    
    select  
    mr.month,
    mr.revenue,
    case when (round((mr.revenue- mean)/std_rev,2)< -1.95) or (round((mr.revenue- mean)/std_rev,2)>1.95)  then 1 else 0 end as anomaly_in
    from monthly_revenue mr
    cross join stats st;



--- retention rate
WITH base AS (
    SELECT 
        c.signup_date,
        strftime('%Y-%m-%d', session_start) AS session_date,
        s.customer_id
    FROM session_transformed s
    LEFT JOIN customer_transformed c 
        ON s.customer_id = c.customer_id 
       where session_start >= signup_date
)

, total_customers as (
   select 
   strftime('%Y-%m-01', signup_date) as signup_mt,
   count(distinct customer_id) as signups
   from customer_transformed
   group by 1
)

, retention as (
SELECT 
    strftime('%Y-%m-01', signup_date) AS month,
    (julianday(session_date) - julianday(signup_date)) as window,
    CASE 
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN  1 AND  7  THEN 'D1-7'
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN  8 AND 14 THEN 'D8-14'
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN 15 AND 30 THEN 'D15-30'
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN 31 AND 60 THEN 'D31-60'
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN 61 AND 90 THEN 'D61-90'
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN 91 AND 120 THEN 'D91-120'
    WHEN (julianday(session_date) - julianday(signup_date)) BETWEEN 121 AND 150 THEN 'D121-150'
    ELSE 'D150+'
  /**
        WHEN datediff('day', signup_date, session_date) BETWEEN  1 AND  7  THEN 'D1-7'
        WHEN datediff('day', signup_date, session_date) BETWEEN  8 AND 14 THEN 'D8-14'
        WHEN datediff('day', signup_date, session_date) BETWEEN 15 AND 30 THEN 'D15-30'
        WHEN datediff('day', signup_date, session_date) BETWEEN 31 AND 60 THEN 'D31-60'
        WHEN datediff('day', signup_date, session_date) BETWEEN 61 AND 90 THEN 'D61-90'
        ELSE 'D90+'
  **/
    END AS retention_bucket,
    count(distinct  customer_id) as retained_cohort
FROM base
group by 1 ,2
  )
  
  select 
  month,
  retention_bucket,
  retained_cohort,
  signups as total_customer
  from retention r
  left join total_customers tc on r.month = tc.signup_mt;


--- lifetime revenue 
with customer_metrics as (
select 
customer_id,
round(sum(total),2) as lifetime_revenue,
round(avg(total),2) as aov,
count(distinct transaction_id) as frequency
from transactions_df_transformed t
where payment_status = 'paid'
group by 1
)

, rank as(
select 
*,
NTILE(10) over(ORDER BY lifetime_revenue DESC) AS decile
from customer_metrics
 )
 
 select 
 *
 from rank
 WHERE decile = 1
 ORDER BY lifetime_revenue DESC;

-- other 

select 
'Q' || ((cast(strftime('%m', created_at) as integer) - 1) / 3 + 1) as quarter,
strftime('%Y-01-01',created_at) as  year,
coalesce(product_name,'unknown') as product,
category,
sum(quantity) as qty,
round(sum(total),2) as revenue
from transactions_df_transformed t
left join Products p on t.product_id=p.product_id
where payment_status = 'paid'
group by 1,2,3,4 ;


select 
'Q' || ((cast(strftime('%m', created_at) as integer) - 1) / 3 + 1) as quarter,
strftime('%Y-01-01',created_at) as  year,
category,
round(unit_price,2) as unit_price,
sum(quantity) as qty,
round(sum(total),2) as revenue
from transactions_df_transformed t
left join Products p on t.product_id=p.product_id
where payment_status = 'paid'
group by 1,2,3 ;






