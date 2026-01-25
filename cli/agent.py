# cli/agent.py

from cli.db import get_conn
from cli.queries import sales, top_products
from datetime import date, timedelta


class DataAgent:
    """
    Minimal, production-minded analytics agent.

    Responsibilities:
    - Discover warehouse schema (marts only)
    - Route supported natural-language questions to deterministic queries
    - Explicitly reject unsupported questions
    """

    def __init__(self):
        self._schema = self._discover_schema()

    # Schema discovery
    def _discover_schema(self) -> dict:
        """Inspect marts tables and columns from information_schema."""
        con = get_conn()
        rows = con.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'marts'
            ORDER BY table_name, ordinal_position
        """).fetchall()

        schema = {}
        for table, column in rows:
            schema.setdefault(table, []).append(column)

        return schema

    def get_schema_summary(self) -> str:
        """Human-readable schema summary."""
        lines = []
        for table, cols in self._schema.items():
            lines.append(f"📊 marts.{table} ({len(cols)} columns)")
            for c in cols:
                lines.append(f"  - {c}")
        return "\n".join(lines)

    # Question routing

    def process_question(self, question: str) -> str:
        """
        Route a natural-language question to a supported analytics primitive.
        This is intentionally rule-based and deterministic.
        """
        q = question.lower()

        # sales / revenue
        if "sales" in q or "revenue" in q:
            return self._handle_sales_question(q)

        # top products
        if "top" in q and "product" in q:
            df = top_products(5)
            lines = ["📦 Top products:"]
            for i, (_, r) in enumerate(df.iterrows(), 1):
                lines.append(
                    f"{i}. Product {r['product_id']}: {r['units_sold']:,} units"
                )
            return "\n".join(lines)

        # unsupported intents
        if "pair" in q:
            return (
                "❌ Product pair analysis is not supported.\n"
                "Reason: marts do not expose order-item level granularity."
            )

        if "anomaly" in q:
            return (
                "❌ Anomaly detection is not supported.\n"
                "Reason: no baseline or alerting semantics are defined."
            )

        return (
            "❓ I don't support that question yet.\n"
            "Try asking about sales, top products, or schema."
        )

    # Helpers
    def _handle_sales_question(self, q: str) -> str:
        """Handle sales-related questions with safe defaults."""
        today = date.today()
        start = today - timedelta(days=90)
        end = today

        df = sales(start.isoformat(), end.isoformat())
        revenue = df.iloc[0]["revenue"]

        if revenue is None:
            return (
                "❌ No sales data available for the requested period.\n"
                "The requested period falls outside the available data range."
            )

        return (
            f"💰 Sales revenue from {start} to {end}: "
            f"${revenue:,.2f}"
        )


# import re
# import pandas as pd
# from typing import Dict, List, Any
# from datetime import datetime, timedelta
# from cli.db import get_conn
# from cli.queries import sales, top_products, product_pairs

# class DataAgent:
#     def __init__(self):
#         self.con = get_conn()
#         self.schema_info = self._discover_schema()
#         self.query_patterns = self._setup_query_patterns()
    
#     def _discover_schema(self) -> Dict[str, Any]:
#         """discover database schema"""
#         schema = {}
#         try:
#             # Discover marts tables
#             tables = self.con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'marts'").fetchall()
#             for table in tables:
#                 table_name = table[0]
#                 # Get column information
#                 columns = self.con.execute(f"DESCRIBE marts.{table_name}").fetchall()
#                 schema[table_name] = {
#                     'columns': [col[0] for col in columns],
#                     'types': dict(zip([col[0] for col in columns], [col[1] for col in columns]))
#                 }
#                 print(f"📊 Discovered table: {table_name} with {len(columns)} columns")
#         except Exception as e:
#             print(f"Schema discovery error: {e}")
        
#         return schema
    
#     def _setup_query_patterns(self) -> Dict[str, Dict]:
#         """Set up query pattern matching"""
#         return {
#             'sales_revenue': {
#                 'keywords': ['sales', 'revenue', 'money', 'earned', 'total'],
#                 'time_keywords': ['quarter', 'month', 'year', 'last', 'this'],
#                 'function': self._handle_sales_query
#             },
#             'product_analysis': {
#                 'keywords': ['product', 'item', 'top', 'best', 'popular'],
#                 'function': self._handle_product_query
#             },
#             'customer_metrics': {
#                 'keywords': ['customer', 'lifetime', 'value', 'average'],
#                 'function': self._handle_customer_query
#             },
#             'product_pairs': {
#                 'keywords': ['together', 'pair', 'combination', 'bought', 'purchased'],
#                 'function': self._handle_product_pairs_query
#             },
#             'anomaly_detection': {
#                 'keywords': ['anomaly', 'unusual', 'strange', 'outlier', 'abnormal'],
#                 'function': self._handle_anomaly_query
#             }
#         }
    
#     def process_question(self, question: str) -> str:
#         """Process natural language question"""
#         print(f"🤔 Processing question: {question}")
#         question_lower = question.lower()
        
#         # Match query type
#         for query_type, pattern in self.query_patterns.items():
#             if any(keyword in question_lower for keyword in pattern['keywords']):
#                 try:
#                     print(f"📝 Matched pattern: {query_type}")
#                     return pattern['function'](question)
#                 except Exception as e:
#                     return f"❌ Error processing {query_type}: {e}"
        
#         # If no match, provide help
#         return self._provide_help()
    
#     def _handle_sales_query(self, question: str) -> str:
#         """Handle sales-related queries"""
#         question_lower = question.lower()
        
#         # Get actual date range
#         if 'last quarter' in question_lower:
#             # Calculate last quarter dates
#             today = datetime.now()
#             if today.month <= 3:
#                 start_date = f"{today.year-1}-10-01"
#                 end_date = f"{today.year-1}-12-31"
#             elif today.month <= 6:
#                 start_date = f"{today.year}-01-01"
#                 end_date = f"{today.year}-03-31"
#             elif today.month <= 9:
#                 start_date = f"{today.year}-04-01"
#                 end_date = f"{today.year}-06-30"
#             else:
#                 start_date = f"{today.year}-07-01"
#                 end_date = f"{today.year}-09-30"
#         elif 'this year' in question_lower:
#             year = datetime.now().year
#             start_date = f"{year}-01-01"
#             end_date = f"{year}-12-31"
#         elif 'last month' in question_lower:
#             today = datetime.now()
#             first_day_this_month = today.replace(day=1)
#             last_day_last_month = first_day_this_month - timedelta(days=1)
#             first_day_last_month = last_day_last_month.replace(day=1)
#             start_date = first_day_last_month.strftime('%Y-%m-%d')
#             end_date = last_day_last_month.strftime('%Y-%m-%d')
#         else:
#             # Default to querying sales for all time
#             start_date = '2020-01-01'
#             end_date = '2025-12-31'
        
#         try:
#             result = sales(start_date, end_date)
#             revenue = result.iloc[0]['revenue']
#             # if revenue is None or revenue == 0:
#             #     return f"💰 No sales data found for the specified period ({start_date} to {end_date})"
            
#             if revenue is None or pd.isna(revenue):
#                 return (
#                     f"💰 No sales data available for {start_date} to {end_date}.\n"
#                     f"The requested period falls outside the available data range."
#                 )
#             return f"💰 Sales revenue: ${revenue:,.2f} (from {start_date} to {end_date})"
#         except Exception as e:
#             return f"❌ Error calculating sales: {e}"
    
#     def _handle_product_query(self, question: str) -> str:
#         """Handle product-related queries"""
#         # Extract number
#         numbers = re.findall(r'\d+', question)
#         n = int(numbers[0]) if numbers else 5
        
#         try:
#             result = top_products(n)
#             if result.empty:
#                 return "📦 No product data found"
            
#             output = f"📦 Top {n} products by units sold:\n"
#             for i, (_, row) in enumerate(result.iterrows(), 1):
#                 output += f"  {i}. Product {row['product_id']}: {row['units_sold']:,} units\n"
#             return output
#         except Exception as e:
#             return f"❌ Error getting top products: {e}"
    
#     def _handle_customer_query(self, question: str) -> str:
#         """Handle customer-related queries"""
#         try:
#             query = """
#                 WITH per_customer AS (
#                     SELECT
#                         user_id,
#                         SUM(total) AS total_spend
#                     FROM marts.fct_orders
#                     GROUP BY user_id
#                 )
#                 SELECT
#                     AVG(total_spend) AS avg_clv,
#                     COUNT(*) AS total_customers,
#                     MIN(total_spend) AS min_clv,
#                     MAX(total_spend) AS max_clv
#                 FROM per_customer
#             """
#             result = self.con.execute(query).fetchdf()
            
#             if result.empty:
#                 return "👤 No customer data found"
            
#             row = result.iloc[0]
#             avg_clv = row['avg_clv']
#             total_customers = row['total_customers']
#             min_clv = row['min_clv']
#             max_clv = row['max_clv']
            
#             return f"👤 Customer Metrics:\n  • Average CLV: ${avg_clv:,.2f}\n  • Total customers: {total_customers:,}\n  • CLV range: ${min_clv:,.2f} - ${max_clv:,.2f}"
#         except Exception as e:
#             return f"❌ Error calculating customer metrics: {e}"
    
#     def _handle_product_pairs_query(self, question: str) -> str:
#         """Handle product pairs queries"""
#         numbers = re.findall(r'\d+', question)
#         top = int(numbers[0]) if numbers else 5
        
#         try:
#             result = product_pairs(top)
#             if result.empty:
#                 return (
#                         "🔗 Product pair analysis requires order-item level granularity.\n"
#                         "This warehouse exposes transaction-level marts.fct_orders only, "
#                         "so true basket analysis is not available."
#                     )
            
#             output = f"🔗 Top {top} product combinations:\n"
#             for i, (_, row) in enumerate(result.iterrows(), 1):
#                 output += f"  {i}. Products {row['product_a']} & {row['product_b']}: purchased together {row['pair_count']} times\n"
#             return output
#         except Exception as e:
#             return f"❌ Error getting product pairs: {e}"
    
#     def _handle_anomaly_query(self, question: str) -> str:
#         """Handle anomaly detection queries"""
#         try:
#             # Simple anomaly detection - find products with unusual sales
#             query = """
#                 WITH product_stats AS (
#                     SELECT 
#                         product_id,
#                         SUM(quantity) as total_quantity,
#                         AVG(SUM(quantity)) OVER () as avg_quantity,
#                         STDDEV(SUM(quantity)) OVER () as std_quantity
#                     FROM marts.fct_orders
#                     GROUP BY product_id
#                 )
#                 SELECT 
#                     product_id,
#                     total_quantity,
#                     avg_quantity,
#                     CASE 
#                         WHEN total_quantity > avg_quantity + 2 * std_quantity THEN 'High Outlier'
#                         WHEN total_quantity < avg_quantity - 2 * std_quantity THEN 'Low Outlier'
#                         ELSE 'Normal'
#                     END as anomaly_type
#                 FROM product_stats
#                 WHERE total_quantity > avg_quantity + 2 * std_quantity 
#                    OR total_quantity < avg_quantity - 2 * std_quantity
#                 ORDER BY ABS(total_quantity - avg_quantity) DESC
#                 LIMIT 10
#             """
            
#             result = self.con.execute(query).fetchdf()
#             if result.empty:
#                 return "🔍 No significant anomalies detected in product sales."
            
#             output = "🔍 Sales anomalies detected:\n"
#             for i, (_, row) in enumerate(result.iterrows(), 1):
#                 output += f"  {i}. Product {row['product_id']}: {row['total_quantity']:,} units ({row['anomaly_type']}) - avg is {row['avg_quantity']:,.0f}\n"
            
#             return output
#         except Exception as e:
#             return f"❌ Error detecting anomalies: {e}"
    
#     def _provide_help(self) -> str:
#         """Provide help information"""
#         return """
#             🤖 I can help you with these types of questions:

#             💰 Sales & Revenue:
#             • "How much in sales did we do last quarter?"
#             • "What's our revenue this year?"
#             • "Sales last month?"

#             📦 Products:
#             • "What are our top 10 products?"
#             • "Which products are most popular?"
#             • "Show me the best 5 items"

#             👤 Customer Analytics:
#             • "What's our average customer lifetime value?"
#             • "Customer metrics"

#             🔗 Product Combinations:
#             • "Which two products are most frequently bought together?"
#             • "Product pairs"

#             🔍 Anomaly Detection:
#             • "Are there any anomalies with how we sell products?"
#             • "Show me unusual sales patterns"

#             💡 Try asking a question!
#         """

#     def get_schema_summary(self) -> str:
#         """Return database schema summary"""
#         summary = "📊 Database Schema Summary:\n"
#         for table_name, info in self.schema_info.items():
#             summary += f"  📋 {table_name}: {len(info['columns'])} columns\n"
#             for col in info['columns'][:5]:  # Show first 5 columns
#                 summary += f"    • {col}\n"
#             if len(info['columns']) > 5:
#                 summary += f"    • ... and {len(info['columns']) - 5} more\n"
#         return summary