# cli/agent.py

from cli.db import get_conn
from cli.queries import sales, top_products, customer_count, top_customers
from datetime import date, timedelta


class DataAgent:
    """
    Minimal, production-minded analytics agent.

    Responsibilities:
    - Discover warehouse schema (marts only)
    - Route supported natural-language questions to deterministic queries
    - Explicitly reject unsupported questions
    """

    INTENTS = [
        {
            "name": "top_customers",
            "keywords": ["top", "most"],
            "entities": ["customer", "user"],
            "handler": "_handle_top_customers",
        },
        {
            "name": "sales",
            "keywords": ["sales", "revenue"],
            "entities": [],
            "handler": "_handle_sales_question",
        },
        {
            "name": "customer_count",
            "keywords": ["how many", "count"],
            "entities": ["customer"],
            "handler": "_handle_customer_count",
        },
    ]

    def __init__(self):
        self._schema = self._discover_schema()
        self._init_data_bounds() 

    def _init_data_bounds(self):
        """
        Discover min/max available transaction_date from marts.fct_orders.
        """
        con = get_conn()
        row = con.execute("""
            SELECT
                MIN(transaction_date) AS min_date,
                MAX(transaction_date) AS max_date
            FROM marts.fct_orders
        """).fetchone()

        # ensure date format
        self.min_date = row[0].date()
        self.max_date = row[1].date()

    # Schema discovery
    def _discover_schema(self) -> dict:
        """Inspect marts tables and columns from information_schema."""
        con = get_conn()

        # table_schema = 'marts' for semantic layer access contrl
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

    def _validate_date_range(self, start: date, end: date):
        if start < self.min_date or end > self.max_date:
            raise ValueError(
                f"Requested period {start} → {end} "
                f"is outside available data range "
                f"({self.min_date} → {self.max_date})"
            )

    def get_schema_summary(self) -> str:
        """Human-readable schema summary."""
        lines = []
        for table, cols in self._schema.items():
            lines.append(f"📊 marts.{table} ({len(cols)} columns)")
            for c in cols:
                lines.append(f"  - {c}")
        return "\n".join(lines)

    def _score_intent(self, intent: dict, question: str) -> int:
        """
        Compute a simple relevance score between a question and an intent.
        """
        score = 0

        for kw in intent["keywords"]:
            if kw in question:
                score += 2

        for ent in intent["entities"]:
            if ent in question:
                score += 1

        return score

    # Question routing
    def process_question(self, question: str) -> str:
        """
        Route a natural-language question to a supported analytics primitive.
        This is intentionally rule-based and deterministic.
        """
        q = question.lower()

        if "data" in q and ("range" in q or "available" in q):
            return self._handle_data_range()

        best_intent = None
        best_score = 0

        for intent in self.INTENTS:
            score = self._score_intent(intent, q)
            if score > best_score:
                best_intent = intent
                best_score = score

        if best_intent and best_score > 0:
            handler = getattr(self, best_intent["handler"])
            try:
                return handler(q)
            except TypeError:
                return handler()

        # if "customer" in q and ("top" in q or "most" in q):
        #     return self._handle_top_customers()

        # # date range / data availability
        # if "data" in q and ("range" in q or "available" in q):
        #     return self._handle_data_range()

        # # sales / revenue
        # if "sales" in q or "revenue" in q:
        #     return self._handle_sales_question(q)

        # # top products
        # if "top" in q and "product" in q:
        #     df = top_products(5)
        #     lines = ["📦 Top products:"]
        #     for i, (_, r) in enumerate(df.iterrows(), 1):
        #         lines.append(
        #             f"{i}. Product {r['product_id']}: {r['units_sold']:,} units"
        #         )
        #     return "\n".join(lines)

        # if "customer" in q and "how many" in q:
        #     return self._handle_customer_count()

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
        today = date.today()
        start = today - timedelta(days=90)
        end = today

        # verify query before running
        try:
            self._validate_date_range(start, end)
        except ValueError as e:
            return f"❌ {str(e)}"

        df = sales(start.isoformat(), end.isoformat())
        revenue = df.iloc[0]["revenue"]

        return (
            f"💰 Sales revenue from {start} to {end}: "
            f"${revenue:,.2f}"
        )

    def _handle_customer_count(self) -> str:
        df = customer_count()
        return f"👥 Total customers: {df.iloc[0]['cnt']:,}"

    def _handle_data_range(self) -> str:
        return (
            f"📅 Available data range: "
            f"{self.min_date} → {self.max_date}"
        )

    def _handle_top_customers(self) -> str:
        df = top_customers(10)
        lines = ["👥 Top customers by order count:"]
        for i, (_, r) in enumerate(df.iterrows(), 1):
            lines.append(
                f"{i}. User {r['user_id']}: {r['order_count']:,} orders"
            )
        return "\n".join(lines)
