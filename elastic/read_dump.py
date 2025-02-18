import os
import mwxml
import wikitextparser as wtp
import glob
from tqdm import tqdm
import constants
from elasticsearch import Elasticsearch

es = Elasticsearch([constants.ELASTICSEARCH_HOST])



def read_dump():
    wiki_dump_file = constants.WIKIPEDIA_XML_BZ2_PATH
    
    max_article_len = int(constants.MAX_ARTICLE_LENGTH)
    max_abstract_len = int(constants.MAX_ABSTRACT_LENGTH)


    def write_fa_dump(dump, _):
        with tqdm(desc="reading articles in dump") as p_bar:
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

                        # Prepare document for Elasticsearch
                        doc = {
                            "title": title,
                            "abstract": abstract if len(abstract) > 0 else None,
                            "content": text
                        }

                        # Index document into Elasticsearch
                        es.index(index=constants.ELASTICSEARCH_INDEX, id=page_id, body=doc)

                        p_bar.update(1)

                    except Exception as e:
                        print(f"An error occurred: {str(e)}")


    # path to the file
    paths = glob.glob(wiki_dump_file)
    try:
        for rev_id, rev_timestamp, delta in mwxml.map(write_fa_dump, paths):
            pass  # Nothing is done here because write_fa_dump works
    except Exception as e:
        print(f"an error occurred: {str(e)}")


def main():    
    read_dump()


if __name__ == "__main__":
    main()
