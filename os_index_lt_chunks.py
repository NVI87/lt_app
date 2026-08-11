import csv
import time
from datetime import datetime
from opensearchpy import OpenSearch

# HOST = "10.248.14.140"
HOST = "localhost"
PORT = 9204
USE_SSL = True

USERNAME = "admin"
PASSWORD = "opensearch-admin123!@#AD"

# - name: INDEX_STORE_ATTACHMENT_ALIAS
#               value: attachment_dev_alias_two
#             - name: INDEX_STORE_ATTACHMENT_CHUNK_ALIAS
#               value: attachment_chunk_dev_alias_two
#             - name: INDEX_STORE_ATTACHMENT_KNOWLEDGE_CHUNK_ALIAS
#               value: attachment_knowledge_chunk_dev_alias_two
#             - name: INDEX_STORE_KNOWLEDGE_ALIAS
#               value: knowledge_dev_alias_two
#             - name: INDEX_STORE_KNOWLEDGE_CHUNK_ALIAS
#               value: knowledge_chunk_dev_alias_two
INDEXES = [
    "attachment_etl_release_tst",
    # "attachment_chunk_load_test_jul_alias",
    "attachment_knowledge_chunk_etl_release_tst_lt_0",
    "knowledge_chunk_etl_release_tst_lt_0",
    "knowledge_etl_release_tst_lt_0",
]

CSV_FILE = "index_counts_release301-0.csv"
INTERVAL_SECONDS = 1


def main():
    client = OpenSearch(
        hosts=[{"host": HOST, "port": PORT}],
        http_auth=(USERNAME, PASSWORD),
        use_ssl=USE_SSL,
        verify_certs=False,
    )

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if f.tell() == 0:
            writer.writerow(["timestamp", "index_name", "doc_count"])

        while True:
            timestamp = datetime.utcnow().isoformat()

            for index_name in INDEXES:
                try:
                    response = client.count(index=index_name)
                    doc_count = response["count"]
                    print(f"[{timestamp}] {index_name}: {doc_count}")
                    writer.writerow([timestamp, index_name, doc_count])
                except Exception as e:
                    writer.writerow([timestamp, index_name, f"ERROR: {e}"])

            f.flush()
            time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
