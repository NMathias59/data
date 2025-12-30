-- KPI: Indicateurs d'optimisation de stock (couverture, vélocité)
-- Description: Calcule mois de couverture, vélocité des ventes et recommandations d'optimisation au niveau produit/magasin
-- Utilité: Sert à produire les recommandations dans `fct_inventory` et `fct_stock_alerts`
-- Colonnes clés retournées: product_id, store_id, monthly_sales_velocity, months_of_stock_coverage, recommendation
-- Notes: Requiert historique des ventes pour estimer vélocité; utiliser rolling windows et coalesce pour robustesse.
{{ config(
    materialized='view'
) }}

-- Stock optimization analysis - Analyse d'optimisation des stocks
with stock_optimization as (
    select
        s.store_id,
        st.store_name,
        s.product_id,
        p.product_name,
        b.brand_name,
        c.category_name,
        s.quantity as current_stock,
        p.list_price,
        s.quantity * p.list_price as stock_value,
        -- Sales velocity (units sold per month - approximated)
        coalesce(
            sum(oi.quantity) / nullif(date_diff('month', min(o.order_date), max(o.order_date)), 0),
            0
        ) as monthly_sales_velocity,
        -- Stock coverage (months of stock remaining)
        case
            when sum(oi.quantity) > 0 then s.quantity / (sum(oi.quantity) / nullif(date_diff('month', min(o.order_date), max(o.order_date)), 0))
            else 999 -- Infinite coverage if no sales
        end as months_of_stock_coverage,
        -- Stock status for optimization
        case
            when s.quantity = 0 then 'Out of Stock'
            when s.quantity <= 2 then 'Critical Stock'
            when months_of_stock_coverage < 1 then 'Low Stock'
            when months_of_stock_coverage > 6 then 'Overstocked'
            else 'Optimal Stock'
        end as stock_optimization_status,
        -- Revenue impact
        case
            when s.quantity = 0 then 'Lost Sales'
            when months_of_stock_coverage < 1 then 'Stockout Risk'
            when months_of_stock_coverage > 6 then 'Capital Tied Up'
            else 'Revenue Optimized'
        end as revenue_impact,
        -- Recommendations
        case
            when s.quantity = 0 then 'Urgent Restock'
            when s.quantity <= 2 then 'Restock Soon'
            when months_of_stock_coverage < 1 then 'Increase Stock'
            when months_of_stock_coverage > 6 then 'Reduce Stock'
            else 'Maintain Current Level'
        end as recommendation
    from {{ ref('stg_bike_shop__stocks') }} s
    join {{ ref('stg_bike_shop__stores') }} st on s.store_id = st.store_id
    join {{ ref('stg_bike_shop__products') }} p on s.product_id = p.product_id
    left join {{ ref('stg_bike_shop__brands') }} b on p.brand_id = b.brand_id
    left join {{ ref('stg_bike_shop__categories') }} c on p.category_id = c.category_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on s.product_id = oi.product_id
    left join {{ ref('stg_bikelocal__orders') }} o on oi.order_id = o.order_id and s.store_id = o.store_id
    group by s.store_id, st.store_name, s.product_id, p.product_name, b.brand_name, c.category_name, s.quantity, p.list_price
)

select * from stock_optimization
order by
    case stock_optimization_status
        when 'Out of Stock' then 1
        when 'Critical Stock' then 2
        when 'Low Stock' then 3
        when 'Overstocked' then 4
        else 5
    end,
    months_of_stock_coverage