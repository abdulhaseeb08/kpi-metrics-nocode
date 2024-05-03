with payment_failed_customers as(
	select s.id, s.status, ili.type
	, pr.unit_amount , p.name
	,  s."cancellation_details" ->> 'reason' ,s.customer,
	       s.latest_invoice, to_timestamp(s.cancel_at) as "cancel_at",
	       to_timestamp(cast(ili."period" ->> 'start' as int)) as "sub_start",
	       to_timestamp(cast(ili."period" ->> 'end' as int)) as "sub_end",
	       to_timestamp(s.canceled_at) as "cancelled_at",
		   round((cast(coalesce(ili."amount",0) as float)-cast(coalesce(ilid."amount",0) as float))/(cast (100 as float))) as total,
		   EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)) "month",
		   EXTRACT(YEAR FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)) "year"
	from subscriptions as s
	inner join subscriptions_cancellation_details as scd 
	on s."_airbyte_subscriptions_hashid" = scd."_airbyte_subscriptions_hashid" 
	inner join invoices as i on i.id = s.latest_invoice 
	inner join invoice_line_items ili on ili.invoice_id = i.id
	   JOIN   PUBLIC.products p
	   ON     (ili."price" ->> 'product') = p.id
	   join public.prices pr
	  on p.id = pr.product
	LEFT JOIN LATERAL (
	    SELECT 
	        CAST((jsonb_array_elements(discount_amounts)->>'amount') AS float) AS amount
	    FROM 
	        invoice_line_items
	    WHERE 
	        ili.id = invoice_line_items.id
	    LIMIT 1
	) AS ilid ON TRUE	
	where s.id in (
	select id from subscriptions 
	where cancel_at is not null
	or canceled_at is not null
	)
	and s."cancellation_details" ->> 'reason' = 'payment_failed'
), customers_list as (
	select * from (
		select i.id, 
		       i."subscription" , 
		       to_timestamp(s."canceled_at") as "canceled_date",
		       i.customer_email ,
		       i.status, 
		       i.customer,
		       to_timestamp(cast(ili."period" ->> 'start' as int)), 
		       to_timestamp(cast(ili."period" ->> 'end' as int)), 
		       EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)) "month",
		       EXTRACT(YEAR FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)) "year",
		       row_number() over (partition by i.customer, EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)), EXTRACT(YEAR FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date))  order by to_timestamp(cast(ili."period" ->> 'start' as int)) desc) as rn
		  from invoices as i
		  inner join invoice_line_items ili on i.id = ili.invoice_id 
		  inner join subscriptions as s on s.id = i."subscription"
		 where i.customer in (select distinct customer from payment_failed_customers)
		) as s
	where rn = 1
)
, customer_invoice_count as (
	select customer, count(customer) as "customer_count"
	from customers_list 
	group by customer
)
select month, year, sum(total) as "delinquent_mrr"
from payment_failed_customers 
where customer in (select customer from customer_invoice_count where customer_count > 1)
group by month, year 
order by year, month