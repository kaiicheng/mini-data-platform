---
title: Product Performance
---

# Product Analytics

Analysis of product performance, category trends, and inventory insights.

## Top Products by Revenue

```sql top_products
SELECT 
    product_name,
    category,
    COUNT(DISTINCT transaction_id) as times_ordered,
    SUM(quantity) as total_units_sold,
    SUM(total) as total_revenue,
    AVG(unit_price) as avg_price
FROM warehouse.fct_orders
GROUP BY product_name, category
ORDER BY total_revenue DESC
LIMIT 50
```

<DataTable data={top_products} rows=20>
    <Column id=product_name/>
    <Column id=category/>
    <Column id=times_ordered fmt='#,##0'/>
    <Column id=total_units_sold fmt='#,##0'/>
    <Column id=total_revenue fmt='$#,##0'/>
    <Column id=avg_price fmt='$#,##0.00'/>
</DataTable>

## Category Performance

```sql category_performance
SELECT 
    category,
    COUNT(DISTINCT product_name) as product_count,
    COUNT(DISTINCT transaction_id) as orders,
    SUM(quantity) as units_sold,
    SUM(total) as revenue,
    AVG(unit_price) as avg_price
FROM warehouse.fct_orders
GROUP BY category
ORDER BY revenue DESC
```

<BarChart 
    data={category_performance}
    x=category
    y=revenue
    title="Revenue by Category"
    yFmt='$#,##0'
/>

<DataTable data={category_performance}>
    <Column id=category/>
    <Column id=product_count fmt='#,##0'/>
    <Column id=orders fmt='#,##0'/>
    <Column id=units_sold fmt='#,##0'/>
    <Column id=revenue fmt='$#,##0'/>
    <Column id=avg_price fmt='$#,##0.00'/>
</DataTable>

## Category Filter

<Dropdown data={category_performance} name=category value=category>
    <DropdownOption value="%" valueLabel="All Categories"/>
</Dropdown>

```sql category_detail
SELECT 
    product_name,
    SUM(quantity) as units_sold,
    SUM(total) as revenue,
    AVG(unit_price) as avg_price,
    COUNT(DISTINCT transaction_id) as order_count
FROM warehouse.fct_orders
WHERE category LIKE '${inputs.category.value}'
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 20
```

<BarChart 
    data={category_detail}
    x=product_name
    y=revenue
    title="Top Products in {inputs.category.label}"
    yFmt='$#,##0'
/>

## Product Sales Over Time

```sql product_trend
SELECT 
    DATE_TRUNC('month', transaction_date) as month,
    category,
    SUM(total) as revenue
FROM warehouse.fct_orders
GROUP BY month, category
ORDER BY month
```

<LineChart 
    data={product_trend}
    x=month
    y=revenue
    series=category
    title="Category Revenue Trends"
    yFmt='$#,##0'
/>

## Price Distribution

```sql price_distribution
SELECT 
    CASE 
        WHEN unit_price < 50 THEN '< $50'
        WHEN unit_price < 100 THEN '$50-100'
        WHEN unit_price < 200 THEN '$100-200'
        WHEN unit_price < 500 THEN '$200-500'
        ELSE '$500+'
    END as price_range,
    COUNT(DISTINCT transaction_id) as order_count,
    SUM(total) as revenue
FROM warehouse.fct_orders
GROUP BY price_range
ORDER BY 
    CASE 
        WHEN unit_price < 50 THEN 1
        WHEN unit_price < 100 THEN 2
        WHEN unit_price < 200 THEN 3
        WHEN unit_price < 500 THEN 4
        ELSE 5
    END
```

<BarChart 
    data={price_distribution}
    x=price_range
    y=order_count
    title="Orders by Price Range"
    yFmt='#,##0'
/>

<BarChart 
    data={price_distribution}
    x=price_range
    y=revenue
    title="Revenue by Price Range"
    yFmt='$#,##0'
/>

## Product Catalog

```sql product_catalog
SELECT DISTINCT
    product_name,
    category,
    AVG(unit_price) as avg_price
FROM warehouse.fct_orders
GROUP BY product_name, category
ORDER BY product_name
```

<DataTable data={product_catalog} search=true rows=20>
    <Column id=product_name/>
    <Column id=category/>
    <Column id=avg_price fmt='$#,##0.00'/>
</DataTable>
