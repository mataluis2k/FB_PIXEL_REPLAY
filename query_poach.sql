with
    payments as ( select * from prod_v2.core.rpt_payment_metrics_detailed where is_test_flag = 0 and payment_status_inclusion_flag = 1 and platform = 'Laravel'), --Updates every 15 minutes.

    dim_users as ( select * from prod_v2.core.dim_users where platform = 'Laravel'), --Every 15 minutes.

    dim_user_geolocation as ( select * from prod_v2.core.dim_user_geolocation), --Every 30 minutes.

    session_starts as ( select * from prod_v2.staging.stg_laravel__session_starts), -- Every 30 minutes.

    laravel_guest_payments as ( select * from prod_v2.staging.stg_laravel__guest_payments), --Every 30 minutes.

    order_line as ( select * from prod_v2.core.rpt_order_line_detailed where platform = 'Laravel'), --Every 30 minutes.

    user_interaction_logs as ( select * from fivetran_database.le_prod_public.user_interaction_logs), --Raw/Source table.

    high_acv_customers as (
        select
            user_id,
            root_session_id,
            count(distinct order_id) as orders
        from laravel_guest_payments
        group by 1, 2
        having orders >= 2
    ),

    pay_users as (
        select
            distinct
            md5(concat(payments.user_id, payments.order_id)) as pk_user_orders,
            payments.user_id,
            payments.email,
            payments.order_id,
            payments.payment_created_at_pt as payment_date
        from payments
        inner join high_acv_customers
            on payments.user_id = high_acv_customers.user_id
        where payment_created_at_pt >= dateadd(day, -7, current_timestamp())
        and payment_created_at_pt < current_timestamp()
    ),

    
    session_starts_enhanced as (
        select
            session_starts.root_session_id,
            session_starts.guest_id,
            session_starts.user_id,
            user_interaction_logs.ip_address,
            user_interaction_logs.server_name,
            user_interaction_logs.url,
            try_parse_json(user_interaction_logs.query_parameters) as qp_json,
            qp_json:"fbclid"::string as fbclid,
            qp_json:"utm_source"::string as utm_source,
            qp_json:"utm_medium"::string as utm_medium,
            qp_json:"utm_campaign"::string as utm_campaign,
            qp_json:"utm_content"::string as utm_content,
            qp_json:"utm_term"::string as utm_term,
            qp_json:"ad_id"::string as ad_id,
            qp_json:"ad_channel"::string as ad_channel,
            coalesce(ad_id, utm_content, utm_campaign) as ad_attribution_id,
            session_starts.created_at_pt,
            lag(session_starts.ad_attribution_id) over (
                partition by session_starts.user_id order by session_starts.created_at_pt, session_starts.root_session_id
            ) as prev_ad_id,
        from session_starts
        inner join user_interaction_logs
            on session_starts.root_session_id = user_interaction_logs.id
        where session_starts.user_id is not null
        and ad_attribution_id is not null
        qualify prev_ad_id is null or ad_attribution_id != prev_ad_id
    ),

    session_starts_user_window as (
        select
            *,
            coalesce(
                lead(created_at_pt) over (
                    partition by user_id
                    order by created_at_pt
                ),
                '2100-10-24 00:00:00.000 +0700'
            ) as next_session_created_at_pt,
        from session_starts_enhanced
    ),

    first_touch as (
        select
            distinct
            pay_users.pk_user_orders,
            pay_users.user_id,
            pay_users.order_id,
            pay_users.email,
            pay_users.payment_date,
            session_starts_user_window.created_at_pt as first_interaction_at_pt,
            session_starts_user_window.ip_address,
            session_starts_user_window.server_name,
            session_starts_user_window.url,
            session_starts_user_window.qp_json,
            session_starts_user_window.fbclid,
            session_starts_user_window.utm_source,
            session_starts_user_window.utm_medium,
            session_starts_user_window.utm_campaign,
            session_starts_user_window.utm_content,
            session_starts_user_window.utm_term,
            session_starts_user_window.ad_id,
            session_starts_user_window.ad_channel
        from pay_users
        inner join session_starts_user_window
            on pay_users.user_id = session_starts_user_window.user_id
            and pay_users.payment_date >= session_starts_user_window.created_at_pt
            and pay_users.payment_date < session_starts_user_window.next_session_created_at_pt
    ),

    order_items as (
        select
            order_id,
            array_agg(
                object_construct(
                    'order_item_sku',
                    order_item_sku,
                    'order_item_name',
                    order_item_name,
                    'quantity',
                    order_line_quantity,
                    'unit_price',
                    order_line_amount
                )
            ) within group (order by order_item_sku) as order_items
        from order_line
        where user_id in ( select user_id from pay_users)
        group by 1
    ),

    final as (
        select
            first_touch.pk_user_orders,
            first_touch.user_id,
            first_touch.email,
            dim_users.phone,
            dim_user_geolocation.final_postal_code as zipcode,
            first_touch.order_id,
            first_touch.payment_date,
            first_touch.first_interaction_at_pt,
            first_touch.ip_address,
            first_touch.server_name,
            first_touch.url as event_source_url,
            first_touch.fbclid,
            first_touch.utm_source,
            first_touch.utm_medium,
            first_touch.utm_campaign,
            first_touch.utm_content,
            first_touch.utm_term,
            first_touch.ad_id,
            first_touch.ad_channel,
            order_items.order_items
        from first_touch
        left join order_items
            on first_touch.order_id = order_items.order_id
        left join dim_users
            on first_touch.user_id = dim_users.user_id
        left join dim_user_geolocation
            on dim_users.customer_identifier = dim_user_geolocation.customer_identifier
        order by first_touch.payment_date, first_touch.order_id
    )

    
select
    *
from final
--where ad_channel = 'facebook'
;    