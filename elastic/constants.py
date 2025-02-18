import os

WIKIPEDIA_XML_BZ2_PATH = os.getenv('WIKIPEDIA_XML_BZ2_PATH', os.path.join("data", "fa_wiki_dump.xml.bz2"))
MAX_ARTICLE_LENGTH = int(os.getenv('MAX_ARTICLE_LENGTH', '-1'))
MAX_ABSTRACT_LENGTH = int(os.getenv('MAX_ABSTRACT_LENGTH', '0'))
ELASTICSEARCH_INDEX = "wiki_articles"
ELASTICSEARCH_HOST = "http://localhost:9200"

