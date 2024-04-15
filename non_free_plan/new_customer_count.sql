with customer_info as (
	select i.id as "invoice_id",
	       i.customer,
	       i.status as "invoice_status",
	       date(to_timestamp(ilipd."start")) as "subscription_start",
	       date(to_timestamp(ilipd."end")) as "subscription_end",
	       pp.name,
	       i.subtotal,
	       EXTRACT(YEAR FROM cast(date(to_timestamp(ilipd."start")) as date)) "year",
	       EXTRACT(MONTH FROM cast(date(to_timestamp(ilipd."start")) as date)) "month",
	       row_number() over (partition by i.customer order by date(to_timestamp(ilipd."start"))) as rn
	from public.invoices as i
	inner join public.invoice_line_items as pili on i.id = pili.invoice_id
	inner join public.invoice_line_items_plan as pilip on pilip."_airbyte_invoice_line_items_hashid" = pili."_airbyte_invoice_line_items_hashid"
	inner join public.products as pp on pp.id = pilip.product
	inner join invoice_line_items_period ilipd on pili."_airbyte_invoice_line_items_hashid" = ilipd."_airbyte_invoice_line_items_hashid"
	where i.status = 'paid'
	and i.subtotal > 0
)
select "month", "year", count(customer) "new_customer_count"
from customer_info
where rn = 1
group by "month", "year"
order by "year", "month"