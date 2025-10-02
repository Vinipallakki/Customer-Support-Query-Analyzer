import os
from langgraph.graph import StateGraph, END
from google.cloud import bigquery
from langchain_google_genai import ChatGoogleGenerativeAI

# 1. Define State
class State(dict):
    query: str
    intent: str
    summary: str

# 2. Setup BigQuery client
bq_client = bigquery.Client()
DATASET = "support_data"
TABLE = "customer_queries"
PROCESSED_TABLE = f"{DATASET}.customer_queries_processed"



# 3. Setup Gemini model
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite",
    temperature=0,
    api_key="KEEP YOUR GEMINI KEY"
)

# --------- NODES ---------

def classify_intent(state: State):
    print(f"🧠 Classifying intent: {state['query']}")
    prompt = f"""
    Classify the intent of this support query into one of:
    [Billing, Technical, General].
    Query: {state['query']}
    """
    result = llm.invoke(prompt)
    return {"intent": result.content.strip()}

def summarize_query(state: State):
    print("📝 Summarizing query")
    prompt = f"Summarize this support query in one sentence:\n{state['query']}"
    result = llm.invoke(prompt)
    return {"summary": result.content.strip()}

def billing_handler(state: State):
    print("💰 Billing issue detected")
    return {"intent": "Billing", "summary": state["summary"]}

def technical_handler(state: State):
    print("🔧 Technical issue detected")
    return {"intent": "Technical", "summary": state["summary"]}

def general_handler(state: State):
    print("📌 General query detected")
    return {"intent": "General", "summary": state["summary"]}

def store_results(state: State):
    print("💾 Storing result in BigQuery")
    table_id = f"{bq_client.project}.{PROCESSED_TABLE}"

    # Ensure table exists
    try:
        bq_client.get_table(table_id)
    except Exception:
        print("⚠️ Processed table not found. Creating...")
        schema = [
            bigquery.SchemaField("query", "STRING"),
            bigquery.SchemaField("intent", "STRING"),
            bigquery.SchemaField("summary", "STRING"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)
        print("✅ Created table:", table_id)

    rows_to_insert = [
        {"query": state["query"], "intent": state["intent"], "summary": state["summary"]}
    ]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("❌ Errors inserting rows:", errors)
    return {}

# --------- BUILD GRAPH ---------

workflow = StateGraph(State)

workflow.add_node("classify", classify_intent)
workflow.add_node("summarize", summarize_query)
workflow.add_node("billing", billing_handler)
workflow.add_node("technical", technical_handler)
workflow.add_node("general", general_handler)
workflow.add_node("store", store_results)

workflow.set_entry_point("classify")
workflow.add_edge("classify", "summarize")

workflow.add_conditional_edges(
    "summarize",
    lambda state: state["intent"].strip().lower().replace("*", ""),
    {
        "billing": "billing",
        "technical": "technical",
        "general": "general",
    },
)


workflow.add_edge("billing", "store")
workflow.add_edge("technical", "store")
workflow.add_edge("general", "store")
workflow.add_edge("store", END)

app = workflow.compile()

# --------- RUN (BATCH MODE) ---------
print("\n🚀 Fetching queries from BigQuery...")

sql = f"SELECT query FROM `{DATASET}.{TABLE}`"
rows = bq_client.query(sql).result()
queries = [{"query": row.query} for row in rows]

print(f"📦 Found {len(queries)} queries. Processing...")

results = app.batch(queries)

print("\n✅ All queries processed and stored in BigQuery.")
for r in results:
    print(r)
