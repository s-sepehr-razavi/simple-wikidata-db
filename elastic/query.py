from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])  # Adjust host if necessary

# Example dictionary where keys are word pairs, and values are fixed IDs
word_pairs_dict = {
    ("مکانیک", "فیزیک"): 101,
    ("مردم", "ایران"): 102
}

# Define the index name
index_name = "wiki_articles"  # Change this to your actual index name

# Dictionary to store word pair -> document IDs mapping
word_pair_to_doc_ids = {}

# Loop through each word pair in the dictionary
for (word1, word2), pair_id in word_pairs_dict.items():
    # Elasticsearch query to find documents containing both words in "content"
    query = {
        "bool": {
            "must": [
                {"match": {"sentence": word1}},  # Match word1 in content
                {"match": {"sentence": word2}}   # Match word2 in content
            ]
        }
    }

    # Perform the search
    response = es.search(index=index_name, query=query, size=1000)  # Adjust size as needed

    # Extract document IDs
    doc_ids = [hit["_id"] for hit in response["hits"]["hits"]]

    # Store the results
    word_pair_to_doc_ids[(word1, word2)] = doc_ids

# Print the final mapping
for pair, doc_ids in word_pair_to_doc_ids.items():
    print(f"Word Pair {pair} -> Document IDs: {doc_ids}")
