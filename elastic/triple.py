import os
import argparse
import json
import itertools
from elasticsearch import Elasticsearch
from tqdm import tqdm

# Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])  # Adjust host if necessary
index = 'wiki_abstracts'  # Replace with your actual index name
output = '/home/sepis_lini/data/relations_output.jsonl'
global qid_to_aliases
qid_to_aliases = {}


def create_alias_pairs(line):
    global qid_to_aliases
    # print(len(qid_to_aliases))
    product = None
    try:
        head = line['qid']
        tail = line['value']
        h = qid_to_aliases[head]
        t = qid_to_aliases[tail]
        # print(h)
        # print(t)
        product = list(itertools.product(h, t))
    except:
        pass
    
    return product


def extract_relation(relations, output_file="relations_output.jsonl"):
    with open(output_file, "a", encoding="utf-8") as outfile:
        global qid_to_aliases
        # print(len(qid_to_aliases))
        for rel in relations:
            # print(rel)
            word_pairs = create_alias_pairs(rel)
            if word_pairs is None:
                continue
            
            relation = rel['property_id']
            for (head, tail) in word_pairs:
                # print(head,tail)
                # print(head, tail, relation)                
                query = {
                    "bool": {
                        "must": [
                            {"match_phrase": {"abstract": head}},
                            {"match_phrase": {"abstract": tail}}
                        ]
                    }
                }
                response = es.search(index=index, query=query, size=1000) # ?? If you have doubt on the size of response, we can rerun this code ...        
                # print(f"Found {len(response['hits']['hits'])} hits for relation {relation} between {head} and {tail}")
                for hit in response["hits"]["hits"]:
                    source = hit["_source"]
                    # Ensure required fields exist
                    article_title = source.get("title", "N/A")                                        
                    data_entry = {
                        "article_title": article_title,                        
                        "head": head,
                        "tail": tail,
                        "relation": relation,
                        "head_id": rel['qid'],
                        "tail_id": rel['value'],
                        "doc_id": hit["_id"]
                    }

                    # Write each entry as a JSON line
                    outfile.write(json.dumps(data_entry, ensure_ascii=False) + "\n")



def main():
    parser = argparse.ArgumentParser(description="Process .jsonl files for aliases and relations.")
    parser.add_argument('--alias_dir', required=True, help='Path to directory with alias .jsonl files')
    parser.add_argument('--relation_dir', required=True, help='Path to directory with relation .jsonl files')
    parser.add_argument('--output', required=True, help='Path to output file')
    args = parser.parse_args()

    alias_dir = args.alias_dir
    relation_dir = args.relation_dir
    output = args.output


    # Load alias files    
    for filename in os.listdir(alias_dir):
        if filename.endswith(".jsonl"):
            filepath = os.path.join(alias_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    obj = json.loads(line.strip())
                    qid_to_aliases.update(obj)    
    print(f"Number of aliases in memory: {len(qid_to_aliases)}")

    # Process relation files
    for filename in tqdm(os.listdir(relation_dir), desc="Processing files"):
        rels = []
        if filename.endswith(".jsonl"):
            filepath = os.path.join(relation_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:                    
                    obj = json.loads(line.strip())                    
                    rels.append(obj)
            extract_relation(rels, output) 
        
if __name__ == "__main__":
    main()