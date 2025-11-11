---
title: E-commerce Analytics Dashboard
---

# E-commerce Analytics Overview

Welcome to the Mini Data Platform analytics dashboard. This dashboard provides insights into sales performance, product analytics, and customer behavior.

## Quick Stats

```sql overall_metrics
SELECT
    COUNT(DISTINCT transaction_id) as total_orders,
    COUNT(DISTINCT user_id) as total_customers,
    COUNT(DISTINCT product_name) as total_products,
    SUM(total) as total_revenue,
    AVG(total) as avg_order_value
FROM warehouse.fct_orders
```

<BigValue 
    data={overall_metrics} 
    value=total_revenue
    fmt='$#,##0'
    title="Total Revenue"
/>

<BigValue 
    data={overall_metrics} 
    value=total_orders
    fmt='#,##0'
    title="Total Orders"
/>

<BigValue 
    data={overall_metrics} 
    value=total_customers
    fmt='#,##0'
    title="Total Customers"
/>

<BigValue 
    data={overall_metrics} 
    value=avg_order_value
    fmt='$#,##0.00'
    title="Avg Order Value"
/>

## Revenue Trend

```sql monthly_revenue
SELECT
    DATE_TRUNC('month', transaction_date) as month,
    SUM(total) as revenue,
    COUNT(DISTINCT transaction_id) as orders,
    COUNT(DISTINCT user_id) as customers
FROM warehouse.fct_orders
GROUP BY month
ORDER BY month
```

<LineChart 
    data={monthly_revenue} 
    x=month
    y=revenue
    title="Monthly Revenue"
    yFmt='$#,##0'
/>

## Top Categories

```sql category_performance
SELECT
    category,
    SUM(total) as revenue,
    COUNT(DISTINCT transaction_id) as orders,
    SUM(quantity) as units_sold
FROM warehouse.fct_orders
GROUP BY category
ORDER BY revenue DESC
LIMIT 10
```

<BarChart 
    data={category_performance}
    x=category
    y=revenue
    title="Revenue by Category"
    yFmt='$#,##0'
/>

## Navigation

- [Sales Overview](/sales) - Daily sales trends and performance
- [Product Performance](/products) - Top products and category analysis
- [Customer Analytics](/customers) - Customer segments and behavior

## Data Sources

This dashboard is powered by:
- **DuckDB Warehouse**: `warehouse/data.duckdb`
- **dbt Models**: `marts.fct_orders`, `marts.dim_customers`, `marts.dim_products`
- **Raw Data Sources**: Postgres (products, transactions), Salesforce (campaigns), Analytics (pageviews)
