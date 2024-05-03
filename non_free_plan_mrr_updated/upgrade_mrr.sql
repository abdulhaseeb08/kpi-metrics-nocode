with single_invoice as (
	select * from (
		select i.id, 
		       i."subscription" , 
		       i.customer_email ,
		       i.status, 
		       i.customer,
		       to_timestamp(cast(ili."period" ->> 'start' as int)) as "start", 
		       to_timestamp(cast(ili."period" ->> 'end' as int)) as "end", 
		       round((cast(coalesce(ili."amount",0) as float)-cast(coalesce(ilid."amount",0) as float))/(cast (100 as float))) as total,
		       EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)) "month",
		       row_number() over (partition by i.customer, EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)), EXTRACT(YEAR FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date))  order by to_timestamp(cast(ili."period" ->> 'start' as int)) desc) as rn
		  from invoices as i
		  inner join invoice_line_items ili on i.id = ili.invoice_id
	LEFT JOIN LATERAL (
	    SELECT 
	        CAST((jsonb_array_elements(discount_amounts)->>'amount') AS float) AS amount
	    FROM 
	        invoice_line_items
	    WHERE 
	        ili.id = invoice_line_items.id
	    LIMIT 1
	) AS ilid ON TRUE
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
          round(i.total/12) as total,
          date(generate_series(date(i."start"), date(i."start") + interval '11 months', '1 month')) AS subscription_start ,
          date(generate_series(date(i."start"), date(i."start") + interval '11 months', '1 month') + interval '1 month') AS subscription_end  
   FROM   single_invoice i
   JOIN   invoice_line_items ili
   ON     i.id = ili.invoice_id
   JOIN   products p
   ON     (ili."price" ->> 'product') = p.id
   join prices pr
   on p.id = pr.product
   where (ili."plan" ->> 'interval') = 'year' and ili.type = 'subscription'
), non_year_customers as (
   SELECT i.customer ,
          i.customer_email ,
          i.status ,
          p."name" as "product_name",
          pr.unit_amount/100 as "unit_amount",
          i.total,
          date(i."start") AS subscription_start ,
          date(i."end") AS subscription_end  
   FROM   single_invoice i
   inner join   invoice_line_items ili on i.id = ili.invoice_id
   inner join   products p on  (ili."price" ->> 'product') = p.id
  join prices pr
  on p.id = pr.product
  where (ili."plan" ->> 'interval') != 'year' and ili.type = 'subscription'
), final_invoices as (
	select *
	from year_customers_to_month
	union 
	select * 
	from non_year_customers
), customer_billling_info as (
	select i.customer,
	       i.status,
	       i.customer_email,
	       i.unit_amount,
	       i.total,
	       EXTRACT(YEAR FROM cast(i."subscription_start" as date)) "start_year",
	       EXTRACT(MONTH FROM cast(i."subscription_start" as date)) "start_month"
	from final_invoices as i 
	where i."subscription_start" != i."subscription_end"
	and i.status = 'paid'
), customer_billing_info_cleaning as (
	select cbi.*,
	       coalesce(lag(cbi.unit_amount) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -111) as "previous_subtotal",
	       coalesce(lead(cbi.unit_amount) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -222) as "next_subtotal",
	       coalesce(lag(cbi.total) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -111) as "previous_total",
	       coalesce(lead(cbi.total) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -222) as "next_total"
	from customer_billling_info as cbi

),upgrades as (
	select cbic.*,
	       cbic.total - cbic.previous_total as upgraded_mrr,
	       case
	       		when cbic.unit_amount > cbic.previous_subtotal and cbic.previous_subtotal not in (-111, 0) then 1
	       		else 0
	       end as upgrade
	from customer_billing_info_cleaning as cbic
)
select start_month, start_year, sum(upgraded_mrr)
from upgrades 
where upgrade = 1
group by start_month, start_year 
order by start_year, start_month

