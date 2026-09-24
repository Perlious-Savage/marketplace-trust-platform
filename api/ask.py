"""Ask-Your-Data: LangGraph text-to-SQL over the published dbt marts, with sqlglot read-only guardrails.

load_schema -> generate -> validate -> execute -> answer   (validate/execute errors loop back to generate, max 3 tries)
Defence in depth: sqlglot allows a single SELECT on analytics.* only, and it runs as a read-only role with a timeout.
"""
import os
import re
from decimal import Decimal
from functools import lru_cache
from typing import TypedDict

import psycopg
import sqlglot
from langgraph.graph import END, StateGraph
from sqlglot import exp

FORBIDDEN = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command, exp.Merge,
             exp.TruncateTable)
MAX_TRIES = 3


def validate(sql: str) -> str:
    try:
        trees = sqlglot.parse(sql, read="postgres")
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"unparseable SQL: {e}") from e
    if len(trees) != 1 or not isinstance(trees[0], exp.Query):
        raise ValueError("only a single SELECT statement is allowed")
    tree = trees[0]
    if tree.find(*FORBIDDEN):
        raise ValueError("write/DDL statements are not allowed")
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    for t in tree.find_all(exp.Table):
        if t.name not in ctes and t.db != "analytics":
            raise ValueError(f"table {t.sql()} is outside the analytics schema")
    for f in tree.find_all(exp.Anonymous):
        if f.name.lower().startswith(("pg_", "dblink", "lo_")):
            raise ValueError(f"function {f.name} is not allowed")
    if not tree.args.get("limit"):
        tree = tree.limit(200)
    return tree.sql(dialect="postgres")


class State(TypedDict, total=False):
    question: str
    schema: str
    sql: str
    error: str | None
    attempts: int
    columns: list
    rows: list
    answer: str
    chart: dict | None


@lru_cache
def llm():
    from langchain.chat_models import init_chat_model
    return init_chat_model(os.environ["LLM_MODEL"], temperature=0)


def text(msg):
    c = msg.content
    return c if isinstance(c, str) else "".join(b.get("text", "") for b in c if isinstance(b, dict))


def load_schema(s: State):
    with psycopg.connect(os.environ["PG_RO_DSN"]) as con:
        rows = con.execute("""
            SELECT c.table_name, obj_description(format('analytics.%I', c.table_name)::regclass),
                   string_agg(c.column_name || ' ' || c.data_type, ', ' ORDER BY c.ordinal_position)
            FROM information_schema.columns c WHERE c.table_schema = 'analytics' GROUP BY 1 ORDER BY 1""").fetchall()
    return {"schema": "\n".join(f"analytics.{t} -- {d or ''}\n  columns: {cols}" for t, d, cols in rows),
            "attempts": 0}


def generate(s: State):
    prompt = (f"You write PostgreSQL for a marketplace analytics warehouse (Dubai classifieds: property, cars, "
              f"classifieds).\nTables:\n{s['schema']}\n\nReturn ONLY one SELECT statement, no markdown, no comments."
              f"\nQuestion: {s['question']}")
    if s.get("error"):
        prompt += f"\n\nYour previous SQL:\n{s['sql']}\nfailed with: {s['error']}\nFix it."
    sql = re.sub(r"^```\w*|```$", "", text(llm().invoke(prompt)).strip()).strip().rstrip(";")
    return {"sql": sql, "attempts": s["attempts"] + 1, "error": None}


def check(s: State):
    try:
        return {"sql": validate(s["sql"])}
    except ValueError as e:
        return {"error": str(e)}


def execute(s: State):
    try:
        with psycopg.connect(os.environ["PG_RO_DSN"]) as con:
            cur = con.execute(s["sql"])
            return {"columns": [d.name for d in cur.description], "rows": [list(r) for r in cur.fetchall()]}
    except psycopg.Error as e:
        return {"error": str(e).strip()}


def answer(s: State):
    cols, rows = s["columns"], s["rows"]
    reply = llm().invoke(f"Question: {s['question']}\nSQL: {s['sql']}\nColumns: {cols}\nRows (first 30): {rows[:30]}"
                         "\nAnswer the question in 2-3 sentences using the numbers. If the data can't answer it, say so.")
    numeric = len(cols) >= 2 and rows and isinstance(rows[0][1], (int, float, Decimal))
    return {"answer": text(reply), "chart": {"type": "bar", "x": cols[0], "y": cols[1]} if numeric else None}


def retry_or(nxt):
    return lambda s: (("generate" if s["attempts"] < MAX_TRIES else END) if s.get("error") else nxt)


g = StateGraph(State)
for name, fn in [("load_schema", load_schema), ("generate", generate), ("validate", check), ("execute", execute),
                 ("answer", answer)]:
    g.add_node(name, fn)
g.set_entry_point("load_schema")
g.add_edge("load_schema", "generate")
g.add_edge("generate", "validate")
g.add_conditional_edges("validate", retry_or("execute"))
g.add_conditional_edges("execute", retry_or("answer"))
g.add_edge("answer", END)
graph = g.compile()


def ask(question: str) -> dict:
    s = graph.invoke({"question": question})
    return {k: s.get(k) for k in ("question", "sql", "columns", "rows", "chart", "answer", "error", "attempts")}
