-- the query below returns customers whose payment intent status is not succeeded
-- and invoice status is open. the results returned are the latest entry of that customer
-- this would give us pending payments

with customer_billing_history as (
	select row_number() over (partition by pi.customer order by date(To_timestamp(pi.period_start)) desc) as "latest_entry",
	       pi.customer,
	       pi.total,
	       pi.status as "i_status",
	       ppi.status as "pi_status",
	       pi.billing_reason,
	       pp.name,
	       date(To_timestamp(pi.period_start)) AS usage_start ,
	       date(To_timestamp(pi.period_end))   AS usage_end,
	       pi.id as "invoice_id",
	       pi.payment_intent
	from public.invoices as pi
	inner join public.invoice_line_items as pili on pi.id = pili.invoice_id
	inner join public.invoice_line_items_price as pilip on pilip."_airbyte_invoice_line_items_hashid" = pili."_airbyte_invoice_line_items_hashid"
	inner join public.products as pp on pp.id = pilip.product
	inner join public.payment_intents as ppi on ppi.invoice = pi.id
)
select * from customer_billing_history
--where cbh.customer = 'cus_Or2t9XMRNSzqtn'
where latest_entry = 1
and i_status != 'paid'
and pi_status != 'succeeded'
and billing_reason != 'subscription_create'


select s.id, s.status, scd.reason ,s.customer,
       s.latest_invoice, date(to_timestamp(s.cancel_at)) as "cancel_at",
       date(to_timestamp(s.canceled_at)) as "cancelled_at"
from subscriptions as s
inner join subscriptions_cancellation_details as scd 
on s."_airbyte_subscriptions_hashid" = scd."_airbyte_subscriptions_hashid" 
where id in (
select id from subscriptions 
where cancel_at is not null
or canceled_at is not null
)