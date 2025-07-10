from elasticsearch import Elasticsearch
from transformers import AutoTokenizer
from collections import defaultdict
from elasticsearch import Elasticsearch
from collections import Counter, defaultdict


es = Elasticsearch("http://localhost:9200")

# Load model once
tokenizer = AutoTokenizer.from_pretrained("HooshvareLab/bert-fa-zwnj-base")
property_set = {"P6": "head of government", "P17": "country", "P19": "place of birth", "P20": "place of death", "P22": "father", "P25": "mother", "P26": "spouse", "P27": "country of citizenship", "P30": "continent", "P31": "instance of", "P35": "head of state", "P36": "capital", "P37": "official language", "P39": "position held", "P40": "child", "P50": "author", "P54": "member of sports team", "P57": "director", "P58": "screenwriter", "P69": "educated at", "P86": "composer", "P102": "member of political party", "P108": "employer", "P112": "founded by", "P118": "league", "P123": "publisher", "P127": "owned by", "P131": "located in the administrative territorial entity", "P136": "genre", "P137": "operator", "P140": "religion", "P150": "contains administrative territorial entity", "P155": "follows", "P156": "followed by", "P159": "headquarters location", "P161": "cast member", "P162": "producer", "P166": "award received", "P170": "creator", "P171": "parent taxon", "P172": "ethnic group", "P175": "performer", "P176": "manufacturer", "P178": "developer", "P179": "series", "P190": "sister city", "P194": "legislative body", "P205": "basin country", "P206": "located in or next to body of water", "P241": "military branch", "P264": "record label", "P272": "production company", "P276": "location", "P279": "subclass of", "P355": "subsidiary", "P361": "part of", "P364": "original language of work", "P400": "platform", "P403": "mouth of the watercourse", "P449": "original network", "P463": "member of", "P488": "chairperson", "P495": "country of origin", "P527": "has part", "P551": "residence", "P569": "date of birth", "P570": "date of death", "P571": "inception", "P576": "dissolved, abolished or demolished", "P577": "publication date", "P580": "start time", "P582": "end time", "P585": "point in time", "P607": "conflict", "P674": "characters", "P676": "lyrics by", "P706": "located on terrain feature", "P710": "participant", "P737": "influenced by", "P740": "location of formation", "P749": "parent organization", "P800": "notable work", "P807": "separated from", "P840": "narrative location", "P937": "work location", "P1001": "applies to jurisdiction", "P1056": "product or material produced", "P1198": "unemployment rate", "P1336": "territory claimed by", "P1344": "participant of", "P1365": "replaces", "P1366": "replaced by", "P1376": "capital of", "P1412": "languages spoken, written or signed", "P1441": "present in work", "P3373": "sibling"}

def search_by_property(index: str, field: str, value: str, use_keyword: bool = False):
    """
    Search an Elasticsearch index for documents where a field matches a value.

    :param index: Name of the Elasticsearch index (e.g., "triples")
    :param field: Field name to query (e.g., "property")
    :param value: Value to search for (e.g., "PuIo3JcBtCojIGCLAxaj")
    :param use_keyword: Whether to use .keyword for exact match (defaults to False)
    :return: List of matching documents (sources only)
    """
    es = Elasticsearch("http://localhost:9200")

    # Optionally append '.keyword' for exact term query
    query_field = f"{field}.keyword" if use_keyword else field

    # Choose between term and match query
    query_type = "term" if use_keyword else "match"

    query = {
        "query": {
            query_type: {
                query_field: value
            }
        },
        "size":1000
    }

    response = es.search(index=index, body=query)
    result = []
    for hit in response["hits"]["hits"]: 
        triple = hit['_source']       
        if triple['relation'] in property_set:
            result.append(triple)
    return result


def resolve_entity_qids(data):
    # Count frequencies of (name, qid) pairs for both head and tail
    name_qid_counter = Counter()

    for entry in data:
        name_qid_counter[(entry['head'], entry['head_id'])] += 1
        name_qid_counter[(entry['tail'], entry['tail_id'])] += 1

    # Determine the most frequent qid for each name
    name_to_qid_freq = defaultdict(list)
    for (name, qid), count in name_qid_counter.items():
        name_to_qid_freq[name].append((qid, count))

    name_to_best_qid = {
        name: max(qid_counts, key=lambda x: x[1])[0]
        for name, qid_counts in name_to_qid_freq.items()
    }

    # Filter the data
    filtered_data = []
    for entry in data:
        correct_head_id = name_to_best_qid[entry['head']]
        correct_tail_id = name_to_best_qid[entry['tail']]
        if entry['head_id'] == correct_head_id and entry['tail_id'] == correct_tail_id:
            filtered_data.append(entry)

    return filtered_data



def find_all_phrase_token_spans(text, phrases):    
    encoding = tokenizer(text, return_offsets_mapping=True, return_attention_mask=False, add_special_tokens=False)

    tokens = tokenizer.convert_ids_to_tokens(encoding['input_ids'])
    offsets = encoding['offset_mapping']

    results = {}  # { phrase: [(start_token_idx, end_token_idx), ...] }

    for phrase in phrases:
        phrase_spans = []
        search_start = 0

        while True:
            phrase_start = text.find(phrase, search_start)
            if phrase_start == -1:
                break
            phrase_end = phrase_start + len(phrase)

            # Find token indices for this occurrence
            token_start = token_end = None
            for idx, (start, end) in enumerate(offsets):
                if start >= phrase_start and token_start is None:
                    token_start = idx
                if end <= phrase_end:
                    token_end = idx
                if start > phrase_end:
                    break

            if token_start is not None and token_end is not None and token_start <= token_end:
                phrase_spans.append((token_start, token_end))

            # Move search start past this occurrence
            search_start = phrase_start + 1

        results[phrase] = phrase_spans if phrase_spans else None

    return tokens, results


def process_doc(doc):
    
    doc_id = doc['_id']
    triples = search_by_property("triples", "doc_id", doc_id, use_keyword=False)
    triples = resolve_entity_qids(triples)

    entity_dict = {}
    relations = []
    for triple in triples:
        entity_dict[triple['head']] = triple['head_id']
        entity_dict[triple['tail']] = triple['tail_id']
        relations.append((triple['head_id'], triple['tail_id'], triple['relation']))
    
    tokens, results = find_all_phrase_token_spans(doc['_source']['abstract'], entity_dict.keys())

    qid_occurence = {}
    for alias in results.keys():
        qid = entity_dict[alias]
        if qid in qid_occurence.keys():
            qid_occurence[qid] += results[alias]
        else:
            qid_occurence[qid] = results[alias]
    
    return {
        'title': doc['_source']['title'],
        'tokens':tokens,
        'qid_pos': qid_occurence,
        'relations': relations
    }



def process_batch(batch):
    for doc in batch:
        abstract = doc["_source"]['abstract']
        if len(abstract.split()) > 128:
            result = process_doc(doc)
            print(result)
        print("="*30)

        
def main():    
            
        

    # Initial search to create a scroll context
    response = es.search(
        index="wiki_abstracts",
        scroll="2m",
        size=1,
        body={
            "query": {
                "match_all": {}
            }
        }
    )

    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]

    # Initial batch processing
    process_batch(hits)

    batch_count = 0
    while hits:
        if batch_count == 10:
            break

        response = es.scroll(
            scroll_id=scroll_id,
            scroll="2m"
        )
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

        process_batch(hits)

        batch_count += 1

    # Always clear scroll in production
    es.clear_scroll(scroll_id=scroll_id)


if __name__ == "__main__":
    main()
