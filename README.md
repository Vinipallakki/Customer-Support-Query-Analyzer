# Customer Support Query Analyzer

A workflow built using **LangGraph** and **Google Gemini** to classify and summarize customer support queries and store the results in **BigQuery**.

---

## Features

- Classifies support queries into **Billing**, **Technical**, or **General** categories.
- Summarizes each query in one sentence.
- Handles queries in **batch mode**.
- Stores processed results in a **BigQuery table** for further analysis.

---

## Prerequisites

- Python 3.10+  
- Google Cloud project with **BigQuery** access  
- Gemini API Key  

---

## Setup

1. Clone the repository:

```bash
git clone <your-repo-url>
cd Customer_Support_Query_Analyzer
