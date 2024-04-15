with single_invoice as (
	select * from (
		select i.id, 
		       i."subscription" , 
		       i.customer_email ,
		       i.status, 
		       i.customer,
		       to_timestamp(ilipd."start"), 
		       to_timestamp(ilipd."end"), 
		       EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)) "month",
		       row_number() over (partition by i.customer, EXTRACT(MONTH FROM cast(to_timestamp(ilipd."start") as date)), EXTRACT(YEAR FROM cast(to_timestamp(ilipd."start") as date))  order by to_timestamp(ilipd."start") desc) as rn
		  from invoices as i
		  inner join invoice_line_items ili on i.id = ili.invoice_id 
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
          p."name" as "product_name",
          pr.unit_amount/100 as "unit_amount",
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
          p."name" as "product_name",
          pr.unit_amount/100 as "unit_amount",
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
), final_invoices as (
	select *
	from year_customers_to_month
	union 
	select * 
	from non_year_customers
)
, customer_billling_info as (
	select i.customer,
	       i.status,
	       i.customer_email,
	       i.unit_amount,
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

),downgrades as (
	select cbic.*,
	       case
	       		when cbic.unit_amount < cbic.previous_subtotal and cbic.unit_amount !=0  then 1
	       		else 0
	       end as downgrade
	from customer_billing_info_cleaning as cbic
)
select start_month, start_year, count(downgrade) 
from downgrades  where downgrade = 1 
group by start_month, start_year  
order by start_year, start_month



