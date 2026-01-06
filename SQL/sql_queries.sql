set search_path to de_week1;

-- Query to get total transaction amount and count of transactions per customer
select c.customer_id, c.full_name, sum(t.amount) as total_amount, count(*) as txn_count
from customers c
join accounts a on a.customer_id = c.customer_id
join transactions t on t.account_id = a.account_id
WHERE t.status = 'posted'
and a.account_type = 'checking'
 and t.ts_event >= '2026-01-01 00:00:00+05:45'
 and t.ts_event < '2026-01-06 00:00:00+05:45'
 group by c.customer_id, c.full_name;

 --using CTEs
set search_path to de_week1;
 with base_txn as (
    select t.transaction_id,
    t.account_id,
    t.ts_event,
    t.amount,
    t.status,
    t.metadata
    from transactions t
    where t.status = 'posted'
 and t.ts_event >= '2026-01-01 00:00:00+05:45'
 and t.ts_event < '2026-01-06 00:00:00+05:45'
 ),
enriched as (
    SELECT
    c.customer_id,
    c.full_name,
    a.account_id,
    a.account_type,
    b.transaction_id,
    b.ts_event,
    b.amount,
    b.metadata
    from base_txn b
    join accounts a on a.account_id = b.account_id
    join customers c on c.customer_id = a.customer_id
    where a.account_type = 'checking'
),
customer_summary as (
    select
    e.customer_id,
    e.full_name,
    sum(e.amount) as total_amount,
    count(*) as txn_count
    from enriched e
    group by e.customer_id, e.full_name
)
select customer_id,
full_name,
total_amount,
txn_count
from customer_summary
order by total_amount desc
limit 5;


--window functions
set search_path to de_week1;
with ranked as (SELECT
    t.*,
    row_number() over (partition by t.account_id order by t.ts_event desc) as rn
    from transactions t
)
SELECT
account_id,
transaction_id,
ts_event,
amount,
status
from ranked
where rn = 1
order by account_id;


set search_path to de_week1;
with spend as (
    SELECT
c.customer_id,
c.full_name,
m.merchant_name,
sum(t.amount) as total_spend
from customers c
join accounts a on a.customer_id = c.customer_id
join transactions t on t.account_id = a.account_id
join merchants m on m.merchant_id = t.merchant_id
where t.status = 'posted'
group by c.customer_id, c.full_name, m.merchant_name
),
ranked as (SELECT
*, rank() over (partition by customer_id order by total_spend desc) as spend_rank
from spend
)
select customer_id, full_name, merchant_name, total_spend, spend_rank
FROM ranked
order by customer_id, spend_rank, total_spend desc;


set search_path to de_week1;
SELECT
transaction_id,
ts_event,
amount,
metadata->>'channel' as channel,
metadata->>'device_id' as device_id,
metadata->>'ip' as ip
from transactions t
order by ts_event
limit 12;

select customer_id, full_name, tags from customers
where tags @> ARRAY['vip']::text[];


SELECT
tag, count(*) as customer_count
from (
SELECT
customer_id,
unnest(tags) as tag
from customers) x
group by tag
order by customer_count desc, tag;
