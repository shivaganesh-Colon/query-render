from fastapi import FastAPI
from pydantic import BaseModel
import ollama
from fastapi.middleware.cors import CORSMiddleware
from google import genai
# import psycopg2
import psycopg2
import json
from psycopg2.extras import RealDictCursor
import re
from psycopg2 import OperationalError
origins = [
    "http://localhost:3000",  # React app origin
]


# The client gets the API key from the environment variable `GEMINI_API_KEY`.
client = genai.Client(api_key="AIzaSyDzK1_o2n3XV3eYffAuvDVuZkldHnrt7ZI")



app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # allow your frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class PromptRequest(BaseModel):
    prompt: str
def extract_sql(gemini_response):
    # If Gemini wraps SQL in `````` blocks, extract the content
    match = re.search(r"``````", gemini_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Otherwise, get the first SQL-looking statement
    semicolon_pos = gemini_response.find(';')
    if semicolon_pos != -1:
        s = gemini_response[:semicolon_pos+1].strip()
        if s.lower().startswith("select") or s.lower().startswith("with"):
            return s
    # Fallback: return whole response (strip for safety)
    return gemini_response.strip()

# Run server: uvicorn backend:app --reload
def clean_sql_query(sql_with_backticks):
    # Match ```sql ... ``` or ``` ... ```
    pattern = r"```(?:sql)?\s*([\s\S]*?)```"
    match = re.search(pattern, sql_with_backticks, re.IGNORECASE)
    if match:
        return match.group(1).strip()  # Extract inner SQL
    else:
        return sql_with_backticks.strip()  # Return original if no match
def query_postgres(sql_query):
    conn = psycopg2.connect(
        dbname="****",
        user="*****",
        password="*****",
        host="localhost",
        port=5432
    )

    clean_query = clean_sql_query(sql_query)
    print("Executing SQL:", clean_query)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(clean_query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print("Error executing query:", e)
 
# def load_schema_from_json(filename="db_schema.json"):
#     with open(filename, "r") as f:
#         schemas = json.load(f)
    
#     # Format as: table_name(column1, column2, ...)
#     lines = []
#     for table, cols in schemas.items():
#         column_names = [col['column_name'] for col in cols]
#         lines.append(f"{table}({', '.join(column_names)})")
    
#     return "\n".join(lines)
# def load_schema_for_prompt_with_synonyms(prompt, filename="testdb_schema.json"):
#     with open(filename, "r") as f:
#         schemas = json.load(f)
    
#     # Define keywords/synonyms mapping to actual table names
#     table_aliases = {
#     "customer": ["customer", "customers", "user", "users", "client", "clients"],
#     "customer_auth": ["auth", "authentication", "login", "user auth", "customer auth"],
#     "customer_password": ["password", "passphrase", "login password", "customer password"],
#     "deposit": ["deposit", "deposits", "savings", "funds", "account deposit"],
#     "emi_payment": ["emi", "emi payment", "installment", "installment payment", "emi payments"],
#     "interest_payment": ["interest payment", "interest", "interest payments"],
#     "interest_payment_reference": ["interest payment reference", "interest reference", "payment reference"],
#     "loan_application": ["loan application", "loan apps", "applications", "loans"],
#     "payment_reference": ["payment reference", "payments", "payment refs"],
#     "personal_loan": ["personal loan", "loans", "loan", "customer loan"],
# }

    
#     prompt_lower = prompt.lower()
#     matched_tables = set()
    
#     # Check for matches of any alias in the prompt
#     for table, aliases in table_aliases.items():
#         for alias in aliases:
#             # Use word boundaries to avoid partial matches
#             if re.search(rf"\b{re.escape(alias)}\b", prompt_lower):
#                 if table in schemas:
#                     matched_tables.add(table)
#                 break  # no need to check other aliases for this table
    
#     # If no tables matched, optionally return empty or full schema
#     if not matched_tables:
#         return ""
    
#     lines = []
#     for table in matched_tables:
#         cols = schemas[table]
#         column_names = [col['column_name'] for col in cols]
#         lines.append(f"{table}({', '.join(column_names)})")
    
#     return "\n".join(lines)
import json
import re

def load_schema_for_prompt_with_synonyms(prompt, filename="testdb_schema.json"):
    with open(filename, "r") as f:
        schemas = json.load(f)
        
    # Define keywords/synonyms mapping to actual table names
    table_aliases = {
        "customer": ["customer", "customers", "user", "users", "client", "clients"],
        "customer_auth": ["auth", "authentication", "login", "user auth", "customer auth"],
        "customer_password": ["password", "passphrase", "login password", "customer password"],
        "deposit": ["deposit", "deposits", "savings", "funds", "account deposit"],
        "emi_payment": ["emi", "emi payment", "installment", "installment payment", "emi payments"],
        "interest_payment": ["interest payment", "interest", "interest payments"],
        "interest_payment_reference": ["interest payment reference", "interest reference", "payment reference"],
        "loan_application": ["loan application", "loan apps", "applications", "loans"],
        "payment_reference": ["payment reference", "payments", "payment refs"],
        "personal_loan": ["personal loan", "loans", "loan", "customer loan"],
    }
    
    # Notes corresponding to tables for JSONB and join instructions
    table_notes = {
        "customer": "The 'personal_info' column is a JSONB object containing additional customer details.",
        "deposit": "The 'customer' column is a JSONB object storing customer information; joins must extract keys accordingly.",
        "personal_loan": (
            "The 'customer', 'guarantor', and 'introducer' columns are JSONB objects containing structured data. "
            "When joining with the customer table, extract 'id' from 'customer' JSONB and cast to bigint, "
            "e.g., customer.id = (personal_loan.customer->>'id')::bigint."
        ),
        "loan_application": "The 'applicant', 'guarantor', and 'introducer' fields are JSONB objects storing structured applicant and guarantor info.",
        "emi_payment": "The 'loan_id' references loans as bigint for joins.",
        "interest_payment": "The 'deposit_id' references the deposit table.",
        "interest_payment_reference": "The 'ip_id' references interest_payment table.",
        "payment_reference": "The 'emi_id' references emi_payment table.",
        "customer_auth": "Links to customer table via 'customer_id' field.",
        "customer_password": "Links to customer_auth table via 'customer_auth_id' field.",
    }
    
    prompt_lower = prompt.lower()
    matched_tables = set()
    
    # Check for matches of any alias in the prompt
    for table, aliases in table_aliases.items():
        for alias in aliases:
            if re.search(rf"\b{re.escape(alias)}\b", prompt_lower):
                if table in schemas:
                    matched_tables.add(table)
                break  # no need to check other aliases for this table
                
    if not matched_tables:
        return ""
    
    lines = []
    notes_lines = []
    
    for table in matched_tables:
        cols = schemas[table]
        column_names = [col['column_name'] for col in cols]
        lines.append(f"{table}({', '.join(column_names)})")
        
        # Append note if exists for the table
        if table in table_notes:
            notes_lines.append(f"- {table}: {table_notes[table]}")
    
    # Add general note at the end about JSONB joins
    general_note = (
        "General Note: For JSONB columns storing related entity info, use JSON extraction with proper casting "
        "for joins in SQL, e.g., (jsonb_column->>'id')::bigint = foreign_table.id. Adjust queries to handle nested JSON fields accordingly."
    )
    notes_lines.append(general_note)
    
    # Combine schema and notes
    full_schema_with_notes = "\n".join(lines) + "\n\nNotes:\n" + "\n".join(notes_lines)
    
    return full_schema_with_notes

@app.post("/api/ollama/")
async def get_ollama_response(request: PromptRequest):
    print(request)
    schema_text = load_schema_for_prompt_with_synonyms(request.prompt)
    print("Schema for prompt:\n", schema_text)
    exampleQuery = f'''
You are an expert in SQL query generation.
Database schema:
{schema_text}
Convert this natural language request into an SQL query:
{request.prompt}
Return only the SQL query, nothing else. Do not include explanations, comments,
or formatting—output must be a valid SQL statement ready to execute.
'''
    print(exampleQuery)
    response = client.models.generate_content(
        model="gemini-2.5-flash", contents=exampleQuery
    )
    print(response)
    main_text = response.candidates[0].content.parts[0].text  # Extract text only
    clean_sql = extract_sql(main_text)  # Pass only the text here
    plain_sql = clean_sql_query(clean_sql)
    data = query_postgres(plain_sql)
    return {"response":{
        "Formated_Query": plain_sql,
        "data": data
    }}