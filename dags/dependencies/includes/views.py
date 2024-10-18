import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def download_views():
    file_name = 'airflow-project/dags/dependencies/includes/views.gz'
    response = requests.get('https://dumps.wikimedia.org/other/pageviews/2024/2024-10/projectviews-20241012-150000')
    with open(file_name, 'wb') as file:
        file.write(response.content)

def fetch_views():
    file_name = '/airflow-project/dags/dependencies/includes/views.txt'
    data = []
    companies = ['Amazon', 'Apple', 'Facebook', 'Google', 'Microsoft']

    with open(file_name, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.strip().split()
            page = ' '.join(parts[:-2]) 
            views = int(parts[-2]) 
            if any(company.lower() in page.lower() for company in companies):
                data.append([page, views])

    extracted_data = pd.DataFrame(data, columns=['page', 'views'])
    extracted_data.to_csv('/airflow-project/dags/dependencies/includes/views.csv', index=False)

def load_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default') 
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_csv('/airflow-project/dags/dependencies/includes/views.csv')
    df.to_sql('pageviews', engine, if_exists='replace', index=False)

def analyse_views():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default') 
    engine = pg_hook.get_sqlalchemy_engine()
    
    query = """
    SELECT page, MAX(views) as max_views
    FROM pageviews
    WHERE page ILIKE '%Amazon%' OR page ILIKE '%Apple%' OR page ILIKE '%Facebook%' 
    OR page ILIKE '%Google%' OR page ILIKE '%Microsoft%'
    GROUP BY page
    ORDER BY max_views DESC
    LIMIT 1;
    """
    result = engine.execute(query).fetchone()
    print(f"Company with highest pageviews: {result['page']} with {result['max_views']} views")
