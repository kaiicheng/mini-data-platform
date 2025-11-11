---
title: Customer Analytics
---

# Customer Insights

Understanding customer behavior, segments, and lifetime value.

## Customer Segments

```sql customer_segments
SELECT 
    customer_segment,
    customer_state,
    COUNT(DISTINCT user_id) as customer_count,
    SUM(total) as total_revenue,
    AVG(total) as avg_order_value,
    SUM(quantity) as total_units
FROM warehouse.fct_orders
GROUP BY customer_segment, customer_state
ORDER BY total_revenue DESC
```

<DataTable data={customer_segments} rows=20>
    <Column id=customer_segment/>
    <Column id=customer_state/>
    <Column id=customer_count fmt='#,##0'/>
    <Column id=total_revenue fmt='$#,##0'/>
    <Column id=avg_order_value fmt='$#,##0.00'/>
    <Column id=total_units fmt='#,##0'/>
</DataTable>

## Revenue by Customer Segment

```sql segment_revenue
SELECT 
    customer_segment,
    COUNT(DISTINCT user_id) as customers,
    COUNT(DISTINCT transaction_id) as orders,
    SUM(total) as revenue,
    AVG(total) as avg_order_value
FROM warehouse.fct_orders
GROUP BY customer_segment
ORDER BY revenue DESC
```

<BarChart 
    data={segment_revenue}
    x=customer_segment
    y=revenue
    title="Revenue by Customer Segment"
    yFmt='$#,##0'
/>

<BarChart 
    data={segment_revenue}
    x=customer_segment
    y=customers
    title="Customer Count by Segment"
    yFmt='#,##0'
/>

## Revenue by State

```sql state_revenue
SELECT 
    customer_state,
    COUNT(DISTINCT user_id) as customers,
    COUNT(DISTINCT transaction_id) as orders,
    SUM(total) as revenue,
    AVG(total) as avg_order_value
FROM warehouse.fct_orders
GROUP BY customer_state
ORDER BY revenue DESC
```

<BarChart 
    data={state_revenue}
    x=customer_state
    y=revenue
    title="Revenue by State"
    yFmt='$#,##0'
/>

## State and Segment Filter

<Dropdown data={state_revenue} name=state value=customer_state>
    <DropdownOption value="%" valueLabel="All States"/>
</Dropdown>

<Dropdown data={segment_revenue} name=segment value=customer_segment>
    <DropdownOption value="%" valueLabel="All Segments"/>
</Dropdown>

```sql filtered_customers
SELECT 
    customer_first_name || ' ' || customer_last_name as customer_name,
    customer_email,
    customer_segment,
    customer_state,
    COUNT(DISTINCT transaction_id) as order_count,
    SUM(total) as lifetime_value,
    AVG(total) as avg_order_value,
    MIN(transaction_date) as first_order,
    MAX(transaction_date) as last_order
FROM warehouse.fct_orders
WHERE customer_state LIKE '${inputs.state.value}'
    AND customer_segment LIKE '${inputs.segment.value}'
GROUP BY customer_name, customer_email, customer_segment, customer_state
ORDER BY lifetime_value DESC
LIMIT 100
```

<BigValue 
    data={filtered_customers}
    value=customer_name
    title="Customers Found"
    fmt='#,##0'
/>

<DataTable data={filtered_customers} rows=20>
    <Column id=customer_name/>
    <Column id=customer_email/>
    <Column id=customer_segment/>
    <Column id=customer_state/>
    <Column id=order_count fmt='#,##0'/>
    <Column id=lifetime_value fmt='$#,##0'/>
    <Column id=avg_order_value fmt='$#,##0.00'/>
    <Column id=first_order/>
    <Column id=last_order/>
</DataTable>

## Top Customers by Lifetime Value

```sql top_customers
SELECT 
    customer_first_name || ' ' || customer_last_name as customer_name,
    customer_email,
    customer_segment,
    customer_state,
    COUNT(DISTINCT transaction_id) as order_count,
    SUM(total) as lifetime_value,
    AVG(total) as avg_order_value,
    SUM(quantity) as total_units_purchased
FROM warehouse.fct_orders
GROUP BY customer_name, customer_email, customer_segment, customer_state
ORDER BY lifetime_value DESC
LIMIT 50
```

<DataTable data={top_customers} rows=20>
    <Column id=customer_name/>
    <Column id=customer_email/>
    <Column id=customer_segment/>
    <Column id=customer_state/>
    <Column id=order_count fmt='#,##0'/>
    <Column id=lifetime_value fmt='$#,##0'/>
    <Column id=avg_order_value fmt='$#,##0.00'/>
    <Column id=total_units_purchased fmt='#,##0'/>
</DataTable>

## Customer Acquisition Over Time

```sql customer_acquisition
SELECT 
    DATE_TRUNC('month', first_order_date) as month,
    COUNT(DISTINCT user_id) as new_customers
FROM (
    SELECT 
        user_id,
        MIN(transaction_date) as first_order_date
    FROM warehouse.fct_orders
    GROUP BY user_id
)
GROUP BY month
ORDER BY month
```

<LineChart 
    data={customer_acquisition}
    x=month
    y=new_customers
    title="New Customers by Month"
    yFmt='#,##0'
/>

## Order Frequency Distribution

```sql order_frequency
SELECT 
    CASE 
        WHEN order_count = 1 THEN '1 order'
        WHEN order_count = 2 THEN '2 orders'
        WHEN order_count BETWEEN 3 AND 5 THEN '3-5 orders'
        WHEN order_count BETWEEN 6 AND 10 THEN '6-10 orders'
        ELSE '10+ orders'
    END as frequency_bucket,
    COUNT(*) as customer_count,
    SUM(lifetime_value) as total_revenue
FROM (
    SELECT 
        user_id,
        COUNT(DISTINCT transaction_id) as order_count,
        SUM(total) as lifetime_value
    FROM warehouse.fct_orders
    GROUP BY user_id
)
GROUP BY frequency_bucket
ORDER BY 
    CASE 
        WHEN order_count = 1 THEN 1
        WHEN order_count = 2 THEN 2
        WHEN order_count BETWEEN 3 AND 5 THEN 3
        WHEN order_count BETWEEN 6 AND 10 THEN 4
        ELSE 5
    END
```

<BarChart 
    data={order_frequency}
    x=frequency_bucket
    y=customer_count
    title="Customers by Order Frequency"
    yFmt='#,##0'
/>

<BarChart 
    data={order_frequency}
    x=frequency_bucket
    y=total_revenue
    title="Revenue by Customer Frequency"
    yFmt='$#,##0'
/>
