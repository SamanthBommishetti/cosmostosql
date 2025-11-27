import logging
import azure.functions as func
from azure.cosmos import CosmosClient
import pyodbc
import time
from datetime import datetime
import os


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Cosmos DB to Azure SQL migration started")
    start_time = time.time()
  
    COSMOS_ENDPOINT   = os.getenv("COSMOS_ENDPOINT")
    COSMOS_KEY        = os.getenv("COSMOS_KEY")
    COSMOS_DB         = os.getenv("COSMOS_DB", "ProductDB")
    COSMOS_CONTAINER  = os.getenv("COSMOS_CONTAINER", "Products")
    SQL_SERVER        = os.getenv("SQL_SERVER")
    SQL_DATABASE      = os.getenv("SQL_DATABASE")
    SQL_USERNAME      = os.getenv("SQL_USERNAME")
    SQL_PASSWORD      = os.getenv("SQL_PASSWORD")
   
    missing = [k for k, v in {
        "COSMOS_ENDPOINT": COSMOS_ENDPOINT,
        "COSMOS_KEY": COSMOS_KEY,
        "SQL_SERVER": SQL_SERVER,
        "SQL_DATABASE": SQL_DATABASE,
        "SQL_USERNAME": SQL_USERNAME,
        "SQL_PASSWORD": SQL_PASSWORD,
    }.items() if not v]
    if missing:
        return func.HttpResponse(f"Missing config: {', '.join(missing)}", status_code=500)
    try:
      
        client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        database = client.get_database_client(COSMOS_DB)
        container = database.get_container_client(COSMOS_CONTAINER)
   
        conn = pyodbc.connect(
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{SQL_SERVER},1433;"
            f"Database={SQL_DATABASE};"
            f"Uid={SQL_USERNAME};"
            f"Pwd={SQL_PASSWORD};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        cursor = conn.cursor()
        total_processed = 0
        failures = 0
        BATCH_SIZE = 500
        product_batch = []
        tag_batch = []
        logging.info("Reading documents from Cosmos DB...")
     
        query = "SELECT * FROM c"
        items = container.query_items(
            query=query,
            enable_cross_partition_query=True,
            max_item_count=1000
        )
        for page in items.by_page():
            docs = list(page)
            for doc in docs:
                try:
                    pid = doc.get("id")
                    if not pid:
                        failures += 1
                        continue
                    name     = doc.get("name")
                    price    = doc.get("price")
                    category = doc.get("category")
                    product_batch.append((pid, name, price, category))
                  
                    for tag in doc.get("tags", []):
                        if tag:
                            tag_batch.append((pid, str(tag)))
                    total_processed += 1
                    if len(product_batch) >= BATCH_SIZE:
                        _insert_products(cursor, product_batch)
                        _insert_tags(cursor, tag_batch)
                        conn.commit()
                        product_batch.clear()
                        tag_batch.clear()
                except Exception as e:
                    failures += 1
                    logging.error(f"Error on doc {doc.get('id')}: {e}")
     
        if product_batch:
            _insert_products(cursor, product_batch)
            _insert_tags(cursor, tag_batch)
            conn.commit()
        duration = time.time() - start_time
        report = (
            f"Migration completed!\n\n"
            f"Total products processed : {total_processed}\n"
            f"Failures                 : {failures}\n"
            f"Duration                 : {duration:.2f} seconds\n"
            f"Finished at            : {datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}"
        )
        logging.info(report)
        conn.close()
        return func.HttpResponse(report, status_code=200, mimetype="text/plain")
    except Exception as ex:
        logging.exception("Fatal error")
        return func.HttpResponse(f"Fatal error: {str(ex)}", status_code=500)
def _insert_products(cur, batch):
    if batch:
        cur.executemany("INSERT INTO Products (Id, Name, Price, Category) VALUES (?, ?, ?, ?)", batch)
def _insert_tags(cur, batch):
    if batch:
        cur.executemany("INSERT INTO ProductTags (ProductId, Tag) VALUES (?, ?)", batch)