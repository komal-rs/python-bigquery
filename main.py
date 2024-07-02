from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

srvc_acct = json.loads(os.getenv("BIGQUERY_SERVICE_ACCT"))
credentials = service_account.Credentials.from_service_account_info(srvc_acct)

client = bigquery.Client(
    credentials=credentials, project="hot-or-not-feed-intelligence"
)


if __name__ == "__main__":
    query = "SELECT DISTINCT event \
            FROM analytics_335143420.test_events_analytics \
            WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND CURRENT_TIMESTAMP();"
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        print(dict(row))
