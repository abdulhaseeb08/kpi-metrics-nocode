WITH all_customers AS
(
       SELECT i.id,
              i.customer,
              i.customer_email,
              i."subscription",
              i.paid,
              i.status,
              p."name",
              pr.unit_amount,
              To_timestamp(i.period_start) AS usage_start,
              To_timestamp(i.period_end)   AS usage_end,
              To_timestamp(ilipd."start")  AS subscription_start,
              To_timestamp(ilipd."end")    AS subscription_end
       FROM   PUBLIC.invoices i
       JOIN   PUBLIC.invoice_line_items ili
       ON     i.id = ili.invoice_id
       JOIN   PUBLIC.invoice_line_items_price ilip
       ON     ili."_airbyte_invoice_line_items_hashid" = ilip."_airbyte_invoice_line_items_hashid"
       JOIN   PUBLIC.products p
       ON     ilip.product = p.id
       JOIN   PUBLIC.prices pr
       ON     p.id = pr.product
       JOIN   invoice_line_items_period ilipd
       ON     ili."_airbyte_invoice_line_items_hashid" = ilipd."_airbyte_invoice_line_items_hashid" ), active_customer_subscriptions AS
(
       SELECT s.id,
              s.customer,
              s.status,
              sp."interval",
              sp.nickname,
              sp.amount,
              s.latest_invoice,
              To_timestamp(s.current_period_start) AS subscription_period_start,
              To_timestamp(s.current_period_end)   AS subscription_period_end
       FROM   PUBLIC.subscriptions s
       JOIN   PUBLIC.subscriptions_plan sp
       ON     s."_airbyte_subscriptions_hashid" = sp."_airbyte_subscriptions_hashid"
       WHERE  (
                     s.status='active'
              OR     s.status='past_due'
              OR     s.status='incomplete')
       AND    (
                     To_timestamp(s.current_period_start)<> To_timestamp(s.current_period_end) ) ), customers_with_historical_data AS
(
          SELECT    ac.customer AS customer_id_from_invoicevoice,
                    ac.id       AS invoice_id,
                    ac.customer AS invoice_customer_id,
                    ac.customer_email,
                    ac."subscription" AS invoce_subscription_id,
                    ac.paid           AS invoice_paid,
                    ac.status         AS invoice_status,
                    ac."name"         AS subscription_name,
                    ac.unit_amount    AS subscription_price,
                    ac.usage_start,
                    ac.usage_end,
                    ac.subscription_start,
                    ac.subscription_end,
                    acs.id             AS subscription_id,
                    acs.customer       AS subscription_customer,
                    acs.status         AS subscripton_status,
                    acs."interval"     AS subscription_plan_interval,
                    acs."nickname"     AS subscription_plan_nickname,
                    acs.amount         AS subscription_plan_amount,
                    acs.latest_invoice AS subscription_latest_invoice,
                    acs.subscription_period_start,
                    acs.subscription_period_end
          FROM      all_customers ac
          LEFT JOIN active_customer_subscriptions acs
          ON        ac.id = acs.latest_invoice
          WHERE     (
                              ac.paid IS true)
          ORDER BY  ac.customer,
                    ac.usage_start ), upgrade_downgrade AS
(
         SELECT   invoice_id,
                  customer_id_from_invoicevoice,
                  invoce_subscription_id,
                  subscription_start,
                  subscription_price,
                  lead(subscription_price) OVER ( partition BY customer_id_from_invoicevoice ORDER BY subscription_start )    AS next_subscription_price,
                  lead(subscription_start) OVER ( partition BY customer_id_from_invoicevoice ORDER BY subscription_start )    AS next_subscription_date,
                  lag(invoce_subscription_id) OVER ( partition BY customer_id_from_invoicevoice ORDER BY subscription_start ) AS prev_subscription
         FROM     customers_with_historical_data ), new_customers AS
(
         SELECT   extract( year FROM subscription_start )  AS "year",
                  extract( month FROM subscription_start ) AS "month",
                  count(
                  CASE
                           WHEN prev_subscription IS NULL THEN 1
                  END ) AS new_customer
         FROM     upgrade_downgrade
         GROUP BY "year",
                  "month"
         ORDER BY "year",
                  "month" ), cumulative_new_customers AS
(
         SELECT   "year",
                  "month",
                  new_customer,
                  sum(new_customer) OVER ( ORDER BY "year", "month" ) AS cumulative_new_customers
         FROM     new_customers ), historical_canceled AS
(
          SELECT    i.id       AS invoice_id,
                    i.customer AS customer_id,
                    s.id       AS subscription_id,
                    i.status   AS invoice_status,
                    s.status   AS subscription_current_status,
                    p."name"   AS product_name,
                    scd.reason AS subscription_cancelation_reason,
                    i.billing_reason,
                    s.latest_invoice AS subscription_latest_invoice_id,
                    s.cancel_at_period_end,
                    i.attempt_count,
                    i.paid                      AS invoice_paid,
                    to_timestamp( ilip."start") AS subscription_start,
                    to_timestamp( ilip."end")   AS subscription_end
                    --lead(to_timestamp( ilip."start"))over(partition by i.customer order by to_timestamp( ilip."start") ) as next_sub_start
          FROM      PUBLIC.invoices i
          LEFT JOIN subscriptions s
          ON        i.id=s.latest_invoice
          LEFT JOIN invoice_line_items ili
          ON        i.id =ili.invoice
          LEFT JOIN invoice_line_items_period ilip
          ON        ili."_airbyte_invoice_line_items_hashid" =ilip."_airbyte_invoice_line_items_hashid"
          LEFT JOIN subscriptions_cancellation_details scd
          ON        s."_airbyte_subscriptions_hashid" =scd."_airbyte_subscriptions_hashid"
          LEFT JOIN PUBLIC.invoice_line_items_price ilipp
          ON        ili."_airbyte_invoice_line_items_hashid" = ilipp."_airbyte_invoice_line_items_hashid"
          LEFT JOIN PUBLIC.products p
          ON        ilipp.product = p.id
          WHERE     s.status!='incomplete_expired'
          AND       s.status!='active'
          AND       s.status!='past_due'
          AND       s.status!='incomplete'
          AND       s.status IS NOT NULL
          ORDER BY  i.customer,
                    ilip."start" ), voluntary_churn AS
(
       SELECT hc.invoice_id,
              hc.customer_id,
              hc.invoice_status,
              hc.product_name,
              hc.subscription_current_status,
              hc.subscription_cancelation_reason ,
              hc.subscription_start
       FROM   historical_canceled hc
       WHERE  customer_id NOT IN
              (
                     SELECT customer
                     FROM   subscriptions s
                     WHERE  s.status='active'
                     OR     s.status='past_due'
                     OR     s.status='incomplete')
       AND    hc.subscription_cancelation_reason='cancellation_requested' ), involuntary_churn AS
(
       SELECT hc.invoice_id,
              hc.customer_id,
              hc.invoice_status,
              hc.product_name,
              hc.subscription_current_status,
              hc.subscription_cancelation_reason ,
              hc.subscription_start
       FROM   historical_canceled hc
       WHERE  customer_id NOT IN
              (
                     SELECT customer
                     FROM   subscriptions s
                     WHERE  s.status='active'
                     OR     s.status='past_due'
                     OR     s.status='incomplete')
       AND    hc.subscription_cancelation_reason='payment_failed' )
SELECT    nc.*,
          ud.upgrades,
          ud.downgrades,
          ( cnc.cumulative_new_customers - (COALESCE (vc.voluntary_churned_customer,0) + COALESCE (ic.involuntary_churned_customer,0)) ) AS existing_customers,
          COALESCE(vc.voluntary_churned_customer,0)                                                                                      AS voluntary_churned_customer,
          COALESCE(ic.involuntary_churned_customer,0)                                                                                    AS involuntary_churned_customer
FROM      new_customers nc
LEFT JOIN
          (
                   SELECT   extract( year FROM next_subscription_date )  AS "year",
                            extract( month FROM next_subscription_date ) AS "month",
                            count(
                            CASE
                                     WHEN next_subscription_price > subscription_price
                                     AND      next_subscription_price IS NOT NULL THEN 1
                            END ) AS upgrades,
                            count(
                            CASE
                                     WHEN next_subscription_price < subscription_price
                                     AND      next_subscription_price IS NOT NULL THEN 1
                            END ) AS downgrades
                   FROM     upgrade_downgrade
                   WHERE    next_subscription_date IS NOT NULL
                   GROUP BY "year",
                            "month" ) ud
ON        nc."year" = ud."year"
AND       nc."month" = ud."month"
LEFT JOIN cumulative_new_customers cnc
ON        nc."year" = cnc."year"
AND       nc."month" = cnc."month"
LEFT JOIN
          (
                   SELECT   extract( year FROM subscription_start )   AS "year",
                            extract ( month FROM subscription_start ) AS "month",
                            count(*)                                  AS voluntary_churned_customer
                   FROM     voluntary_churn
                   GROUP BY "year",
                            "month" ) vc
ON        nc."year" = vc."year"
AND       nc."month" = vc."month"
LEFT JOIN
          (
                   SELECT   extract( year FROM subscription_start )   AS "year",
                            extract ( month FROM subscription_start ) AS "month",
                            count(*)                                  AS involuntary_churned_customer
                   FROM     involuntary_churn
                   GROUP BY "year",
                            "month" ) ic
ON        nc."year" = ic."year"
AND       nc."month" = ic."month"
ORDER BY  nc."year",
          nc."month";

