from google import genai
# import psycopg2
import psycopg2
from psycopg2 import OperationalError
# The client gets the API key from the environment variable `GEMINI_API_KEY`.
client = genai.Client(api_key="AIzaSyDzK1_o2n3XV3eYffAuvDVuZkldHnrt7ZI")

response = client.models.generate_content(
    model="gemini-2.5-flash", contents="get the results from users table in sql"
)






def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="myappdb",
            user="shiva",
            password="Study@123",
            host="localhost",
            port=5432
        )
        print("Database connected successfully")
        return conn
    except OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None

# Test connection
connection = connect_db()
print(connection)