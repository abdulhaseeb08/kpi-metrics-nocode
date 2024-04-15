with payment_failed_customers as(
	select s.id, s.status, ili.type
	, pr.unit_amount , p.name
	, scd.reason ,s.customer,
	       s.latest_invoice, to_timestamp(s.cancel_at) as "cancel_at",
	       to_timestamp(ilipd."start") as "sub_start",
	       to_timestamp(ilipd."end") as "sub_end",
	       to_timestamp(s.canceled_at) as "cancelled_at",
		       EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)) "month",
		       EXTRACT(YEAR FROM cast(to_timestamp(ilipd."start") as date)) "year"
	from subscriptions as s
	inner join subscriptions_cancellation_details as scd 
	on s."_airbyte_subscriptions_hashid" = scd."_airbyte_subscriptions_hashid" 
	inner join invoices as i on i.id = s.latest_invoice 
	inner join invoice_line_items ili on ili.invoice_id = i.id 
	   join invoice_line_items_period ilipd on ili."_airbyte_invoice_line_items_hashid" =ilipd."_airbyte_invoice_line_items_hashid"
	   JOIN   PUBLIC.invoice_line_items_price ilip
	   ON     ili."_airbyte_invoice_line_items_hashid" =ilip."_airbyte_invoice_line_items_hashid"
	   JOIN   PUBLIC.products p
	   ON     ilip.product = p.id
	   join public.prices pr
	  on p.id = pr.product
	where s.id in (
	select id from subscriptions 
	where cancel_at is not null
	or canceled_at is not null
	)
	and scd.reason = 'payment_failed'
), customers_list as (
	select * from (
		select i.id, 
		       i."subscription" , 
		       to_timestamp(s."canceled_at") as "canceled_date",
		       i.customer_email ,
		       i.status, 
		       i.customer,
		       to_timestamp(ilipd."start"), 
		       to_timestamp(ilipd."end"), 
		       EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)) "month",
		       EXTRACT(YEAR FROM cast(to_timestamp(ilipd."start") as date)) "year",
		       row_number() over (partition by i.customer, EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)), EXTRACT(YEAR FROM cast(to_timestamp(ilipd."start") as date))  order by to_timestamp(ilipd."start") desc) as rn
		  from invoices as i
		  inner join invoice_line_items ili on i.id = ili.invoice_id 
		  inner join subscriptions as s on s.id = i."subscription"
		   join invoice_line_items_period ilipd 
		   on ili."_airbyte_invoice_line_items_hashid" =ilipd."_airbyte_invoice_line_items_hashid"
		 where i.customer in (select distinct customer from payment_failed_customers)
		) as s
	where rn = 1
)
, customer_invoice_count as (
	select customer, count(customer) as "customer_count"
	from customers_list 
	group by customer
)
select month, year, count(distinct customer)
from payment_failed_customers 
where customer in (select customer from customer_invoice_count where customer_count > 1)
group by month, year 
order by year, month







