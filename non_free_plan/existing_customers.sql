with single_invoice as (
	select * from (
		select i.id, 
		       i."subscription" , 
			   coalesce(scd.reason, 'not_cancelled') as reason,
		       to_timestamp(s."canceled_at") as "canceled_date",
		       i.customer_email ,
		       i.status, 
		       i.customer,
		       to_timestamp(ilipd."start"), 
		       to_timestamp(ilipd."end"), 
		       EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)) "month",
		       row_number() over (partition by i.customer, EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)), EXTRACT(YEAR FROM cast(to_timestamp(ilipd."start") as date))  order by to_timestamp(ilipd."start") desc) as rn
		  from invoices as i
		  inner join invoice_line_items ili on i.id = ili.invoice_id 
		  inner join subscriptions as s on s.id = i."subscription"
          left join subscriptions_cancellation_details as scd 
          on s."_airbyte_subscriptions_hashid" = scd."_airbyte_subscriptions_hashid" 
		   join invoice_line_items_period ilipd 
		   on ili."_airbyte_invoice_line_items_hashid" =ilipd."_airbyte_invoice_line_items_hashid"
		 where i.status = 'paid' and ili.type = 'subscription'
		) as s
	where rn = 1 
), year_customers_to_month AS
(
   SELECT i.customer ,
          i.customer_email ,
          i.status ,
          i."canceled_date",
          p."name" as "product_name",
          pr.unit_amount/100 as "unit_amount",
		  i.reason,
          date(generate_series(date(To_timestamp(ilipd."start")), date(To_timestamp(ilipd."start")) + interval '11 months', '1 month')) AS subscription_start ,
          date(generate_series(date(To_timestamp(ilipd."start")), date(To_timestamp(ilipd."start")) + interval '11 months', '1 month') + interval '1 month') AS subscription_end  
   FROM   single_invoice i
   JOIN   PUBLIC.invoice_line_items ili
   ON     i.id = ili.invoice_id
   JOIN   PUBLIC.invoice_line_items_price ilip
   ON     ili."_airbyte_invoice_line_items_hashid" =ilip."_airbyte_invoice_line_items_hashid"
   JOIN   PUBLIC.products p
   ON     ilip.product = p.id
   join public.prices pr
   on p.id = pr.product
   join invoice_line_items_period ilipd on ili."_airbyte_invoice_line_items_hashid" =ilipd."_airbyte_invoice_line_items_hashid"
   join invoice_line_items_plan ilipp on ilipp."_airbyte_invoice_line_items_hashid" = ili."_airbyte_invoice_line_items_hashid"
   where ilipp.interval = 'year' and ili.type = 'subscription'
), non_year_customers as (
   SELECT i.customer ,
          i.customer_email ,
          i.status ,
          i."canceled_date",
          p."name" as "product_name",
          pr.unit_amount/100 as "unit_amount",
		  i.reason,
          date(To_timestamp(ilipd."start")) AS subscription_start ,
          date(To_timestamp(ilipd."end")) AS subscription_end  
   FROM   single_invoice i
   inner join   PUBLIC.invoice_line_items ili on i.id = ili.invoice_id
   inner join   PUBLIC.invoice_line_items_price ilip on ili."_airbyte_invoice_line_items_hashid" =ilip."_airbyte_invoice_line_items_hashid"
   inner join   PUBLIC.products p on  ilip.product = p.id
   join public.prices pr
   on p.id = pr.product
   join invoice_line_items_period ilipd on ili."_airbyte_invoice_line_items_hashid" =ilipd."_airbyte_invoice_line_items_hashid"
   join invoice_line_items_plan ilipp on ilipp."_airbyte_invoice_line_items_hashid" = ili."_airbyte_invoice_line_items_hashid"
   where ilipp.interval != 'year' and ili.type = 'subscription'
), temp_invoices as (
	select *
	from year_customers_to_month
	union 
	select * 
	from non_year_customers
), final_invoices as (
	select * from temp_invoices
	where subscription_end <= coalesce(canceled_date, subscription_end)
), customer_billling_info as (
	select i.customer,
	       i.status,
	       i.customer_email,
	       i.unit_amount,
	       i.subscription_end,
		   i.reason,
	       EXTRACT(YEAR FROM cast(i."subscription_start" as date)) "start_year",
	       EXTRACT(MONTH FROM cast(i."subscription_start" as date)) "start_month",
	       EXTRACT(YEAR FROM cast(i."subscription_end" as date)) "end_year",
	       EXTRACT(MONTH FROM cast(i."subscription_end" as date)) "end_month"
	from final_invoices as i 
	where i."subscription_start" != i."subscription_end"
	and i.status = 'paid'
), customer_billing_info_cleaning as (
	select cbi.*,
	       coalesce(lag(cbi.unit_amount) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -111) as "previous_subtotal",
	       coalesce(lead(cbi.unit_amount) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -222) as "next_subtotal"	       
	from customer_billling_info as cbi

), voluntary_churn as (
	select cbic.*,
	       case
	       		when cbic.previous_subtotal > 0 and cbic.unit_amount = 0 then 1
	       		when cbic.next_subtotal < 0 
	       		and cbic.unit_amount  > 0 
	       		and cbic."start_year" = EXTRACT(year FROM current_timestamp)
	       		and cbic."start_month" = EXTRACT(month FROM current_timestamp)
	       		then 0
	       		when cbic.next_subtotal < 0 
	       		and cbic.unit_amount  > 0
	       		and CONCAT(cbic."start_year", '-', cbic."start_month") != CONCAT(EXTRACT(year FROM current_timestamp), '-', EXTRACT(month FROM current_timestamp))
	       		and (select current_timestamp) > cbic.subscription_end
	       		then 2
	       		else 0
	       end as churn
	from customer_billing_info_cleaning as cbic
	where cbic.reason != 'payment_failed'
), churn_one as (
	select start_month, start_year, count(customer) as voluntary_churn
	from voluntary_churn
	where churn = 1
	group by start_month, start_year 
	order by start_year, start_month
), churn_two as (
	select end_month, end_year, count(customer) as voluntary_churn
	from voluntary_churn
	where churn = 2
	group by end_month, end_year 
	order by end_year, end_month
), voluntary_churn_cte as (
	select co.start_month, co.start_year, co.voluntary_churn + coalesce(ct.voluntary_churn, 0) as "voluntary_churn"
	from churn_one as co
	left join churn_two as ct on co.start_month = ct.end_month and co.start_year = ct.end_year
),upgrades as (
	select cbic.*,
	       case
	       		when cbic.unit_amount > cbic.previous_subtotal and cbic.previous_subtotal not in (-111, 0) then 1
	       		else 0
	       end as upgrade
	from customer_billing_info_cleaning as cbic
), upgrade_cte as ( 
	select start_month, start_year, count(upgrade) as upgrade_count
	from upgrades 
	where upgrade = 1
	group by start_month, start_year 
	order by start_year, start_month
),downgrades as (
	select cbic.*,
	       case
	       		when cbic.unit_amount < cbic.previous_subtotal and cbic.unit_amount !=0  then 1
	       		else 0
	       end as downgrade
	from customer_billing_info_cleaning as cbic
), downgrade_cte as (
	select start_month, start_year, count(downgrade) as downgrade_count
	from downgrades  where downgrade = 1 
	group by start_month, start_year  
	order by start_year, start_month
), reactivate as (
	select cbic.*,
	       case
	       		when cbic.unit_amount > 0 and cbic.previous_subtotal = 0 then 1
	       		else 0
	       end as reactivation
	from customer_billing_info_cleaning as cbic
), reactivate_cte as (
	select start_month, start_year, count(customer) as reactivate_count
	from reactivate
	where reactivation = 1
	group by start_month, start_year
	order by start_year, start_month
),payment_failed_customers as(
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
), delinquent_churn_cte as (
	select month, year, count(distinct customer) as delinquent_churn
	from payment_failed_customers 
	where customer in (select customer from customer_invoice_count where customer_count > 1)
	group by month, year 
	order by year, month
), customer_info as (
	select i.customer,
	       i.status,
	       i."subscription_start",
	       i."subscription_end",
	       i.unit_amount,
	       EXTRACT(YEAR FROM cast(subscription_start as date)) "year",
	       EXTRACT(MONTH FROM cast(subscription_start as date)) "month",
	       row_number() over (partition by i.customer order by date(subscription_start)) as rn
	from final_invoices as i
	where i.status = 'paid' and i.unit_amount > 0
), new_customer_count_cte as (
	select "month", "year", count(customer) "new_customers"
	from customer_info
	where rn = 1
	group by "month", "year"
	order by "year", "month"
)
select ncc.*
      ,coalesce(vcc.voluntary_churn, 0) as voluntary_churn
      ,coalesce(uc.upgrade_count, 0) as upgrades
      ,coalesce(dc.downgrade_count, 0) as downgrades
      ,coalesce(rc.reactivate_count, 0) as reactivations
      ,coalesce(dcc.delinquent_churn, 0) as delinquent_churn
      ,sum(ncc.new_customers - coalesce(vcc.voluntary_churn, 0) + coalesce(rc.reactivate_count, 0) - coalesce(dcc.delinquent_churn, 0)) over (order by ncc.year, ncc.month) as existing_customers
from new_customer_count_cte as ncc
left join voluntary_churn_cte as vcc on ncc.month = vcc.start_month and ncc.year = vcc.start_year
left join upgrade_cte as uc on ncc.month = uc.start_month and ncc.year = uc.start_year
left join downgrade_cte as dc on ncc.month = dc.start_month and ncc.year = dc.start_year
left join reactivate_cte as rc on ncc.month = rc.start_month and ncc.year = rc.start_year
left join delinquent_churn_cte as dcc on ncc.month = dcc.month and ncc.year = dcc.year







