WITH year_customers_to_month AS
(
   SELECT i.id ,
          i.customer ,
          i.customer_email ,
          i."subscription" ,
          i.paid ,
          i.status ,
          p."name" ,
          (cast(pr.unit_amount as FLOAT)/100)/12 as unit_amount,
          (cast(i.total_excluding_tax as FLOAT)/100)/12 as amount,
          To_timestamp(i.period_start) AS usage_start ,
          To_timestamp(i.period_end)   AS usage_end  ,
          generate_series(date(To_timestamp(ilipd."start")), date(To_timestamp(ilipd."start")) + interval '11 months', '1 month') AS subscription_start ,
          To_timestamp(ilipd."end") AS subscription_end  
   FROM   PUBLIC.invoices i
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
   where ilipp.interval = 'year'
   and charge not in (select charge from refunds)
),
non_year_customers as (
   SELECT i.id ,
          i.customer ,
          i.customer_email ,
          i."subscription" ,
          i.paid ,
          i.status ,
          p."name" ,
          pr.unit_amount/100 as unit_amount,
          i.total_excluding_tax/100 as amount,
          To_timestamp(i.period_start) AS usage_start ,
          To_timestamp(i.period_end)   AS usage_end  ,
          To_timestamp(ilipd."start") AS subscription_start ,
          To_timestamp(ilipd."end") AS subscription_end  
   FROM   PUBLIC.invoices i
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
   where ilipp.interval != 'year'
   and charge not in (select charge from refunds)
)
select *
from year_customers_to_month
union 
select * 
from non_year_customers