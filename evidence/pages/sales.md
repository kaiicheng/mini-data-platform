---
title: Sales Overview
---

# Sales Performance

Detailed analysis of sales trends, order patterns, and revenue performance.

## Daily Sales Trends

```sql daily_sales

SELECT
    DATE_TRUNC('day', transaction_date) as order_date,
    COUNT(DISTINCT transaction_id) as total_orders,
    SUM(quantity) as total_units_sold,
    SUM(total) as total_revenue,
    AVG(total) as avg_order_value
FROM warehouse.fct_orders
GROUP BY order_date
ORDER BY order_date
```

<LineChart 
    data={daily_sales} 
    x=order_date
    y=total_revenue
    title="Daily Revenue"
    yFmt='$#,##0'
/>

<LineChart 
    data={daily_sales} 
    x=order_date
    y=total_orders
    title="Daily Order Count"
    yFmt='#,##0'
/>

## Year Selector

<Dropdown name=year>
    <DropdownOption value=% valueLabel="All Years"/>
    <DropdownOption value=2020/>
    <DropdownOption value=2021/>
    <DropdownOption value=2022/>
    <DropdownOption value=2023/>
    <DropdownOption value=2024/>
</Dropdown>

```sql filtered_monthly

SELECT
    DATE_TRUNC('month', transaction_date) as month,
    SUM(total) as revenue,
    COUNT(DISTINCT transaction_id) as orders,
    AVG(total) as avg_order_value
FROM warehouse.fct_orders
WHERE CAST(YEAR(transaction_date) AS VARCHAR) LIKE '${inputs.year.value}'
GROUP BY month
ORDER BY month
```

<LineChart 
    data={filtered_monthly} 
    x=month
    y=revenue
    title="Monthly Revenue - {inputs.year.label}"
    yFmt='$#,##0'
/>

## Sales by Day of Week

```sql day_of_week

SELECT
    DAYNAME(transaction_date) as day_name,
    DAYOFWEEK(transaction_date) as day_num,
    COUNT(DISTINCT transaction_id) as orders,
    SUM(total) as revenue,
    AVG(total) as avg_order_value
FROM warehouse.fct_orders
GROUP BY day_name, day_num
ORDER BY day_num
```

<BarChart 
    data={day_of_week}
    x=day_name
    y=revenue
    title="Revenue by Day of Week"
    yFmt='$#,##0'
/>

## Sales by State

```sql state_sales

SELECT
    customer_state as state,
    COUNT(DISTINCT transaction_id) as orders,
    SUM(total) as revenue,
    COUNT(DISTINCT user_id) as customers
FROM warehouse.fct_orders
GROUP BY state
ORDER BY revenue DESC
```

<BarChart 
    data={state_sales}
    x=state
    y=revenue
    title="Revenue by State"
    yFmt='$#,##0'
/>

<DataTable data={state_sales} rows=20>
    <Column id=state/>
    <Column id=orders fmt='#,##0'/>
    <Column id=revenue fmt='$#,##0'/>
    <Column id=customers fmt='#,##0'/>
</DataTable>

## Recent Orders

```sql recent_orders

SELECT
    transaction_date,
    transaction_id,
    customer_first_name || ' ' || customer_last_name as customer_name,
    product_name,
    quantity,
    total
FROM warehouse.fct_orders
ORDER BY transaction_date DESC
LIMIT 100
```

<DataTable data={recent_orders} rows=20>
    <Column id=transaction_date/>
    <Column id=transaction_id/>
    <Column id=customer_name/>
    <Column id=product_name/>
    <Column id=quantity fmt='#,##0'/>
    <Column id=total fmt='$#,##0.00'/>
</DataTable>
