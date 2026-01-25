import re
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime, timedelta
from cli.db import get_conn
from cli.queries import sales, top_products, product_pairs

class DataAgent:
    def __init__(self):
        self.con = get_conn()
        self.schema_info = self._discover_schema()
        self.query_patterns = self._setup_query_patterns()
    
    def _discover_schema(self) -> Dict[str, Any]:
        """discover database schema"""
        schema = {}
        try:
            # Discover marts tables
            tables = self.con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'marts'").fetchall()
            for table in tables:
                table_name = table[0]
                # Get column information
                columns = self.con.execute(f"DESCRIBE marts.{table_name}").fetchall()
                schema[table_name] = {
                    'columns': [col[0] for col in columns],
                    'types': dict(zip([col[0] for col in columns], [col[1] for col in columns]))
                }
                print(f"📊 Discovered table: {table_name} with {len(columns)} columns")
        except Exception as e:
            print(f"Schema discovery error: {e}")
        
        return schema

    def _setup_query_patterns(self) -> Dict[str, Dict]:
        """Set up query pattern matching"""
        return {
            'sales_revenue': {
                'keywords': ['sales', 'revenue', 'money', 'earned', 'total'],
                'time_keywords': ['quarter', 'month', 'year', 'last', 'this'],
                'function': self._handle_sales_query
            },
            'product_analysis': {
                'keywords': ['product', 'item', 'top', 'best', 'popular'],
                'function': self._handle_product_query
            },
            'customer_metrics': {
                'keywords': ['customer', 'lifetime', 'value', 'average'],
                'function': self._handle_customer_query
            },
            'product_pairs': {
                'keywords': ['together', 'pair', 'combination', 'bought', 'purchased'],
                'function': self._handle_product_pairs_query
            },
            'anomaly_detection': {
                'keywords': ['anomaly', 'unusual', 'strange', 'outlier', 'abnormal'],
                'function': self._handle_anomaly_query
            }
        }