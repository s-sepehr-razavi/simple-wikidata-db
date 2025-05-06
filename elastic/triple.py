import os
import json
import itertools
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])  # Adjust host if necessary
index = 'wiki_articles_clone'
output = '/home/ubuntu/relations_output.jsonl'
qid_to_aliases = {}



def create_alias_pairs(line):
    product = None
    try:
        head = line['qid']
        tail = line['value']
        h = qid_to_aliases[head]
        t = qid_to_aliases[tail]
        product = list(itertools.product(h, t))
    except:
        pass
    
    return product

import json

def extract_relation(relations, output_file="relations_output.jsonl"):
    with open(output_file, "a", encoding="utf-8") as outfile:
        for rel in relations:
            word_pairs = create_alias_pairs(rel)
            if word_pairs is None:
                continue
            
            relation = rel['property_id']
            for (head, tail) in word_pairs:
                query = {
                    "bool": {
                        "must": [
                            {"match": {"sentence": head}},
                            {"match": {"sentence": tail}}
                        ]
                    }
                }
                response = es.search(index=index, query=query, size=1000)

                for hit in response["hits"]["hits"]:
                    source = hit["_source"]
                    # Ensure required fields exist
                    article_title = source.get("article_title", "N/A")
                    sentence_id = source.get("sentence_id", "N/A")

                    data_entry = {
                        "article_title": article_title,
                        "sentence_id": sentence_id,
                        "head": head,
                        "tail": tail,
                        "relation": relation
                    }

                    # Write each entry as a JSON line
                    outfile.write(json.dumps(data_entry, ensure_ascii=False) + "\n")



def main():    

    directory = "/home/ubuntu/data/wikidata/aliases"  # your directory containing .jsonl files
         
    for filename in os.listdir(directory):
        if filename.endswith(".jsonl"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    obj = json.loads(line.strip())
                    qid_to_aliases.update(obj)  # each line has one key-value pair

    # Now `qid_to_aliases` is your combined dictionary
    print(f"Number of aliases in the memory:{len(qid_to_aliases)}")


    directory = "/home/ubuntu/data/wikidata/entity_rels"  # your directory containing .jsonl files

    for filename in os.listdir(directory):
        rels = []
        if filename.endswith(".jsonl"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    obj = json.loads(line.strip())
                    rels.append(obj)
        extract_relation(rels, output)

if __name__ == "__main__":    
    main()
    # input()