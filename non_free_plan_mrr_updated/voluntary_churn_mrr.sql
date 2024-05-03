with single_invoice as (
	select * from (
		select i.id, 
		       coalesce(s."cancellation_details" ->> 'reason', 'not_cancelled') as reason,
		       i."subscription" , 
		       to_timestamp(s."canceled_at") as "canceled_date",
		       i.customer_email ,
		       i.status, 
		       i.customer,
		       round((cast(coalesce(ili."amount",0) as float)-cast(coalesce(ilid."amount",0) as float))/(cast (100 as float))) as total,
		       to_timestamp(cast(ili."period" ->> 'start' as int)) as "start", 
		       to_timestamp(cast(ili."period" ->> 'end' as int)) "end", 
		       EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)) "month",
		       row_number() over (partition by i.customer, EXTRACT(MONTH FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date)), EXTRACT(YEAR FROM cast(to_timestamp(cast(ili."period" ->> 'start' as int)) as date))  order by to_timestamp(cast(ili."period" ->> 'start' as int)) desc) as rn
		  from invoices as i
		  inner join invoice_line_items ili on i.id = ili.invoice_id 
		  inner join subscriptions as s on s.id = i."subscription"
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
	where rn = 1 and reason != 'payment_failed'
), year_customers_to_month AS
(
   SELECT i.customer ,
          i.customer_email ,
          i.status ,
          i."canceled_date",
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
          i."canceled_date",
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
	       i.total,
	       i.subscription_end,
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
	       coalesce(lead(cbi.unit_amount) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -222) as "next_subtotal",
	       coalesce(lag(cbi.total) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -111) as "previous_total",
	       coalesce(lead(cbi.total) over (partition by cbi.customer order by cbi."start_year", cbi."start_month"), -222) as "next_total"
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
), churn_mrr_one as (
	select start_month, start_year, sum(previous_total) as mrr_one
	from voluntary_churn
	where churn = 1
	group by start_month, start_year
	order by start_year, start_month
), churn_mrr_two as (
	select end_month, end_year, sum(total) as mrr_two
	from voluntary_churn
	where churn = 2
	group by end_month, end_year
	order by end_year, end_month
)
select co.start_month, co.start_year, -1 * (co.mrr_one + coalesce(ct.mrr_two, 0)) as "voluntary_churn_mrr"
from churn_mrr_one as co
left join churn_mrr_two as ct on co.start_month = ct.end_month and co.start_year = ct.end_year









