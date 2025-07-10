import argparse
import json
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

def create_index(es, index_name):
    mapping = {
        "mappings": {
            "properties": {
                "article_title": {"type": "text", "analyzer": "standard"},
                "head": {"type": "keyword"},
                "tail": {"type": "keyword"},
                "relation": {"type": "keyword"},
                "head_id": {"type": "keyword"},
                "tail_id": {"type": "keyword"},
                "doc_id": {"type": "keyword"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"✅ Index '{index_name}' created.")
    else:
        print(f"⚠️ Index '{index_name}' already exists.")

def count_lines(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)

def generate_actions(file_path, index_name):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            yield {
                "_index": index_name,                
                "_source": doc
            }

def bulk_index(es, index_name, file_path):
    total_lines = count_lines(file_path)
    print(f"📦 Indexing {total_lines} documents from {file_path} into '{index_name}'...")

    actions = generate_actions(file_path, index_name)
    progress = tqdm(actions, total=total_lines, desc="Indexing")

    helpers.bulk(es, progress)
    print("✅ Bulk indexing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create index and bulk index JSONL data into Elasticsearch.")
    parser.add_argument("--index", required=True, help="Name of the Elasticsearch index to create/use")
    parser.add_argument("--file", required=True, help="Path to the JSONL file to be indexed")

    args = parser.parse_args()

    es = Elasticsearch("http://localhost:9200")

    create_index(es, args.index)
    bulk_index(es, args.index, args.file)
