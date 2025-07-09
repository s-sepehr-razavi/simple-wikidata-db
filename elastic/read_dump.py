import os
import mwxml
import wikitextparser as wtp
import glob
from tqdm import tqdm
import constants
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from piraye import NormalizerBuilder
from hazm import sent_tokenize


es = Elasticsearch([constants.ELASTICSEARCH_HOST])
normalizer = NormalizerBuilder().alphabet_fa().digit_fa().punctuation_fa().tokenizing().remove_extra_spaces().build()


def normalize_tokenize(text):
    text = normalizer(text)
    return sent_tokenize(text)


def create_index(abstract=True):
    """Create an Elasticsearch index with a mapping."""
    if not abstract:
        if not es.indices.exists(index=constants.ELASTICSEARCH_INDEX):
            mappings = {
                "mappings": {
                    "properties": {
                        "article_title": {"type": "text"},
                        "sentence_id": {"type": "integer"},
                        "sentence": {"type": "text"}
                    }
                }
            }
            es.indices.create(index=constants.ELASTICSEARCH_INDEX, body=mappings)
    else:
        if not es.indices.exists(index=constants.ELASTICSEARCH_INDEX):
        # Define optional mappings and settings
            mappings = {            
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "abstract": {"type": "text"},
                        "url": {"type": "keyword"},
                        "timestamp": {"type": "date"}
                    }
                }
            }
            es.indices.create(index=constants.ELASTICSEARCH_INDEX, body=mappings)

def convert_to_doc(title, text):
    """Split text into sentences and index them."""
    sentences = sent_tokenize(text)
    for idx, sentence in enumerate(sentences):
        doc = {
            "article_title": title,
            "sentence_id": idx,
            "sentence": sentence
        }        
        yield doc

def read_dump():    
    wiki_dump_file = constants.WIKIPEDIA_XML_BZ2_PATH
    
    max_article_len = int(constants.MAX_ARTICLE_LENGTH)
    max_abstract_len = int(constants.MAX_ABSTRACT_LENGTH)

    create_index()
    
    def write_fa_dump(dump, _): 
        print(constants.ELASTICSEARCH_INDEX)                
        with tqdm(desc="reading articles in dump") as p_bar:
            x = 0
            for page in dump:
                for revision in page:
                    if revision.page.namespace != 0:
                        continue

                    # Getting page ID and title
                    page_id = revision.page.id
                    title = revision.page.title

                    try:
                        # Parsing wikitext
                        parsed = wtp.parse(revision.text)
                        first_section = parsed.sections[0].plain_text()
                        abstract = first_section.replace('\0', '')

                        # Skip redirects
                        if (
                            first_section.startswith("#تغییر_مسیر")
                            or first_section.startswith("#تغییرمسیر")
                            or first_section.startswith("#تغییر مسیر")
                            or first_section.startswith("#redirect")
                            or first_section.startswith("#REDIRECT")
                        ):
                            continue

                        # Process text content
                        text = parsed.plain_text().replace('\0', '')
                        if max_article_len != -1 and len(text) > max_article_len:
                            text = text[:max_article_len]


                        # for ok, action in streaming_bulk(client=es, index=constants.ELASTICSEARCH_INDEX, actions=convert_to_doc(title, text)):
                        #     pass                      
                        doc = {
                            "title":title,
                            "abstract": normalizer.normalize(abstract)
                        }                        
                        response = es.index(index=constants.ELASTICSEARCH_INDEX, document=doc)

                        p_bar.update(1)
                    
                    except Exception as e:
                        print(f"An error occurred: {str(e)}")

        print("All the data has been successfully processed!")

    # path to the file
    paths = glob.glob(wiki_dump_file)
    try:
        print([constants.WIKIPEDIA_XML_BZ2_PATH])          
        for id, namespace, title in mwxml.map(write_fa_dump, [constants.WIKIPEDIA_XML_BZ2_PATH]):
            pass
    except Exception as e:
        print(f"an error occurred: {str(e)}")


def main():    
    read_dump()


if __name__ == "__main__":
    main()
