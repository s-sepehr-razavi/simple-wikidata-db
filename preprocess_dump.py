""" Wikidata Dump Processor

This script preprocesses the raw Wikidata dump (in JSON format) and sorts triples into 8 "tables": labels, descriptions, aliases, entity_rels, external_ids, entity_values, qualifiers, and wikipedia_links. See the README for more information on each table.

Example command:

python3 preprocess_dump.py \
    --input_file /lfs/raiders8/0/lorr1/wikidata/raw_data/latest-all.json.gz \
    --out_dir data/processed

"""
import argparse
import multiprocessing
from multiprocessing import Queue, Process
from pathlib import Path
import time
import sys
import os

from simple_wikidata_db.preprocess_utils.reader_process import count_lines, read_data
from simple_wikidata_db.preprocess_utils.worker_process import process_data
from simple_wikidata_db.preprocess_utils.writer_process import write_data
from SPARQLWrapper import SPARQLWrapper, JSON



# https://rdflib.github.io/sparqlwrapper/

def language_restricted_properties(language, out_dir):
  endpoint_url = "https://query.wikidata.org/sparql"

  query = """SELECT ?property WHERE {
    ?property a wikibase:Property.
    ?property rdfs:label ?propertyLabel.
    FILTER(LANG(?propertyLabel) = "%s").
  }
  """ % (language)


  def get_results(endpoint_url, query):
      user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
      # TODO adjust user agent; see https://w.wiki/CX6
      sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
      sparql.setQuery(query)
      sparql.setReturnFormat(JSON)
      return sparql.query().convert()
  
  path = os.path.join(out_dir, language + "Props.txt")
  properties = []

  if os.path.exists(path):
      with open(path, 'r') as file:
          for p in file:
            properties.append(p.strip())
  else:
      
    results = get_results(endpoint_url, query)

    for result in results["results"]["bindings"]:
        properties.append(result['property']['value'].split('/')[-1])
  
    with open(path, 'w') as file:
        for p in properties:
            file.write(p + '\n')
  
  return set(properties)

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, required=True, help='path to gz wikidata json dump')
    parser.add_argument('--out_dir', type=str, required=True, help='path to output directory')
    parser.add_argument('--language_id', type=str, default='en', help='language identifier')
    parser.add_argument('--processes', type=int, default=90, help="number of concurrent processes to spin off. ")
    parser.add_argument('--batch_size', type=int, default=10000)
    parser.add_argument('--num_lines_read', type=int, default=-1,
                        help='Terminate after num_lines_read lines are read. Useful for debugging.')
    parser.add_argument('--num_lines_in_dump', type=int, default=-1, help='Number of lines in dump. If -1, we will count the number of lines.')
    parser.add_argument("--mini", action='store_true', help='generating minimized version of the output')
    return parser


def main():    
    start = time.time()    
    args = get_arg_parser().parse_args()    
    print(f"ARGS: {args}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    path_to_count = os.path.join(out_dir, 'readObjCount.txt')
    pre_read_lines = 0
    if os.path.exists(path_to_count):
        # Open and read the file
        with open(path_to_count, 'r') as file:
            pre_read_lines = (int)(file.read())  # You can use read(), readline(), or readlines() depending on your need
            # print(pre_read_lines)

    input_file = Path(args.input_file)
    assert input_file.exists(), f"Input file {input_file} does not exist"


    max_lines_to_read = args.num_lines_read
    if args.num_lines_in_dump <= 0:
        print("Counting lines")
        total_num_lines = count_lines(input_file, max_lines_to_read)
    else:
        total_num_lines = args.num_lines_in_dump

    print("Starting processes")
    maxsize = 10 * args.processes

    # Queues for inputs/outputs
    output_queue = Queue(maxsize=maxsize)
    work_queue = Queue(maxsize=maxsize)

    # List of persian properties
    restricted_properties = language_restricted_properties(args.language_id, out_dir)    

    # Processes for reading/processing/writing
    num_lines_read = multiprocessing.Value("i", 0)
    read_process = Process(
        target=read_data,
        args=(input_file, num_lines_read, max_lines_to_read, work_queue, pre_read_lines)
    )

    read_process.start()
    
    write_process = Process(
        target=write_data,
        args=(out_dir, args.batch_size, total_num_lines, output_queue, args.mini, pre_read_lines)
    )
    write_process.start()

    work_processes = []
    # print(args.mini)
    for _ in range(max(1, args.processes-2)):
        work_process = Process(
            target=process_data,
            args=(args.language_id, work_queue, output_queue, restricted_properties, args.mini)
        )
        work_process.daemon = True
        work_process.start()
        work_processes.append(work_process)

    read_process.join() 
    print(f"Done! Read {num_lines_read.value} lines")
    # Cause all worker process to quit
    for work_process in work_processes:
        work_queue.put(None)
    # Now join the work processes
    for work_process in work_processes:
        work_process.join()
    output_queue.put(None)
    write_process.join()

    print(f"Finished processing {num_lines_read.value} in {time.time() - start}s")


if __name__ == "__main__":
    main()
    # input()