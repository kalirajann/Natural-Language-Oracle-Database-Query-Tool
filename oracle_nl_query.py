import os
import oracledb
from openai import OpenAI
from dotenv import load_dotenv
from typing import List, Dict, Any
import json

# Load environment variables
load_dotenv()

class OracleNLQuery:
    def __init__(self):
        # Initialize OpenAI client without any arguments
        self.client = OpenAI()  # The API key will be read from environment variable automatically
        
        # Database connection parameters
        self.connection_params = {
            'user': os.getenv('ORACLE_USER'),
            'password': os.getenv('ORACLE_PASSWORD'),
            'dsn': os.getenv('ORACLE_DSN')
        }
        
        # Initialize database connection
        self.connection = None
        self.connect()

    def connect(self):
        """Establish connection to Oracle database"""
        try:
            self.connection = oracledb.connect(**self.connection_params)
            print("Successfully connected to Oracle database")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def get_table_schema(self) -> str:
        """Get the database schema information"""
        cursor = self.connection.cursor()
        schema_info = []
        
        # Query to get table and column information
        query = """
        SELECT table_name, column_name, data_type 
        FROM user_tab_columns 
        ORDER BY table_name, column_id
        """
        
        try:
            cursor.execute(query)
            for row in cursor:
                schema_info.append(f"Table: {row[0]}, Column: {row[1]}, Type: {row[2]}")
        finally:
            cursor.close()
            
        return "\n".join(schema_info)

    def natural_language_to_sql(self, nl_query: str) -> str:
        """Convert natural language query to SQL using OpenAI"""
        schema_info = self.get_table_schema()
        
        prompt = f"""Given the following database schema:
        {schema_info}
        
        Convert this natural language query to SQL:
        {nl_query}
        
        Return only the SQL query without any explanation or additional text.
        Make sure the SQL query is safe and follows Oracle SQL syntax.
        Do not include a semicolon at the end of the query.
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Convert natural language queries to SQL."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            # Strip any semicolon from the end of the query
            sql_query = response.choices[0].message.content.strip()
            return sql_query.rstrip(';')
        except Exception as e:
            print(f"Error converting query: {e}")
            raise

    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_query)
            columns = [col[0] for col in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            return results
        finally:
            cursor.close()

    def query(self, nl_query: str) -> List[Dict[str, Any]]:
        """Main method to process natural language query"""
        try:
            sql_query = self.natural_language_to_sql(nl_query)
            print(f"Generated SQL query: {sql_query}")
            return self.execute_query(sql_query)
        except Exception as e:
            print(f"Error processing query: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")

def main():
    nl_query = None  # Initialize to None so it's always defined
    try:
        nl_query = OracleNLQuery()
        
        # Example natural language query
        query = "who is getting more salary in the Managers "
        results = nl_query.query(query)
        
        print("\nResults:")
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if nl_query:  # Only try to close if it was successfully created
            nl_query.close()

if __name__ == "__main__":
    main() 