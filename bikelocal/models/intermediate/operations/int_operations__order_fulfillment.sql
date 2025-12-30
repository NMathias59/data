-- KPI: Fulfillment (traitement et expédition des commandes)
-- Description: Mesure les délais (days_to_ship), statut d'exécution (On Time/Late/Overdue) et autres métriques opérationnelles par commande
-- Utilité: Base pour `fct_operations_performance` pour évaluer qualité de service et délais
-- Colonnes clés retournées: order_id, customer_id, staff_id, order_date, shipped_date, days_to_ship, fulfillment_status
-- Notes: Important pour analyser SLA et identifier points d'amélioration dans la chaîne d'exécution.
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

-- Order fulfillment metrics - Métriques de traitement des commandes
with order_fulfillment as (
    select
        o.order_id as order_id,
        o.customer_id as customer_id,
        c.first_name,
        c.last_name,
        o.staff_id as staff_id,
        st.first_name as staff_first_name,
        st.last_name as staff_last_name,
        s.store_name,
        o.order_date,
        o.required_date,
        o.shipped_date,
        o.order_status,
        sum(oi.quantity) as total_items,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as order_total,
        case
            when o.shipped_date is null and o.order_date < today() - interval '7' day then 'Overdue'
            when o.shipped_date is null then 'Pending'
            when o.shipped_date <= o.required_date then 'On Time'
            when o.shipped_date > o.required_date then 'Late'
            else 'Unknown'
        end as fulfillment_status,
        case
            when o.shipped_date is not null then date_diff('day', o.order_date, o.shipped_date)
            else null
        end as days_to_ship,
        case
            when o.shipped_date is not null then date_diff('day', o.required_date, o.shipped_date)
            else null
        end as days_past_due
    from {{ ref('stg_bikelocal__orders') }} o
    left join {{ ref('stg_bike_shop__customers') }} c on o.customer_id = c.customer_id
    left join {{ ref('stg_bike_shop__staffs') }} st on o.staff_id = st.staff_id
    left join {{ ref('stg_bike_shop__stores') }} s on o.store_id = s.store_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    group by o.order_id, o.customer_id, c.first_name, c.last_name, o.staff_id,
             st.first_name, st.last_name, s.store_name, o.order_date, o.required_date,
             o.shipped_date, o.order_status
)

select * from order_fulfillment