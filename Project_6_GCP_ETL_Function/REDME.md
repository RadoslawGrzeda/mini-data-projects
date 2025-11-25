Case

Several times a day, the company that processes our transaction system uploads transaction data to a specific location in Google Cloud Storage. Our goal is to process this data and load it into a designated table in BigQuery.

To achieve this, I created a Cloud Function that is triggered whenever a file is uploaded to a specific bucket and blob path. The incoming files include both e-commerce and in-store transaction data, but our system should process only e-commerce transactions and append them to a target BigQuery table.