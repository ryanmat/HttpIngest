import logging
import os
import json
import io
import gzip
import psycopg2
import azure.functions as func

# Create a global FunctionApp instance
app = func.FunctionApp()

@app.function_name(name="HttpIngest")
@app.route(route="HttpIngest", methods=["POST"])
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing HTTPS request from the Data Publisher.')

    try:
        # Read the raw bytes from the request body
        body_bytes = req.get_body()
        
        # Check if the payload is gzipped using the Content-Encoding header
        if req.headers.get("Content-Encoding", "").lower() == "gzip":
            logging.info("Content-Encoding is gzip. Decompressing payload.")
            with gzip.GzipFile(fileobj=io.BytesIO(body_bytes)) as f:
                decompressed_bytes = f.read()
            # Decode the bytes to a string and parse JSON
            req_body = json.loads(decompressed_bytes.decode('utf-8'))
        else:
            # If not gzipped, assume JSON is sent uncompressed
            req_body = req.get_json()
    except Exception as e:
        logging.error(f"Error parsing JSON payload: {e}")
        return func.HttpResponse("Invalid JSON payload", status_code=400)

    # Retrieve the PostgreSQL connection string from environment variables
    conn_str = os.getenv("POSTGRES_CONN_STR")
    if not conn_str:
        return func.HttpResponse("Database connection string is not set.", status_code=500)

    try:
        # Connect to PostgreSQL Flexible Server
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        # Insert the JSON data into table 'json_data'
        insert_query = "INSERT INTO json_data (data) VALUES (%s) RETURNING id;"
        cursor.execute(insert_query, (json.dumps(req_body),))
        inserted_id = cursor.fetchone()[0]
        conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Database error: {e}")
        return func.HttpResponse("Database error: " + str(e), status_code=500)

    return func.HttpResponse(f"Data inserted with id: {inserted_id}", status_code=200)
