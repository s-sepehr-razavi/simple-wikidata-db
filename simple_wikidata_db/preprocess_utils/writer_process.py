import shutil
from multiprocessing import Queue
from pathlib import Path
from typing import Dict, Any, List
import time
import ujson
import os

TABLE_NAMES = [
    'labels', 'descriptions', 'aliases', 'external_ids', 'entity_values', 'qualifiers', 'wikipedia_links', 'entity_rels'
]

MINIMIZED_TABLE_NAMES = ['aliases', 'entity_rels']


class Table:
    def __init__(self, path: Path, batch_size: int, table_name: str, index:int, cur_num_lines:int):
        self.table_dir = path / table_name
        if not self.table_dir.exists():
        #     shutil.rmtree(self.table_dir)
            self.table_dir.mkdir(parents=True, exist_ok=False)

        self.index = index
        self.cur_num_lines = cur_num_lines
        self.batch_size = batch_size
        self.cur_file = self.table_dir / f"{self.index:d}.jsonl"
        self.cur_file_writer = None

    def write(self, json_value: List[Dict[str, Any]]):
        print(self.cur_file)
        if self.cur_file_writer is None:
            self.cur_file_writer = open(self.cur_file, 'a', encoding='utf-8')
        for json_obj in json_value: #?
            # print(json_obj)
            self.cur_file_writer.write(ujson.dumps(json_obj, ensure_ascii=False) + '\n')
        self.cur_num_lines += 1
        if self.cur_num_lines >= self.batch_size:
            self.cur_file_writer.close()
            self.cur_num_lines = 0
            self.index += 1
            self.cur_file = self.table_dir / f"{self.index:d}.jsonl"
            self.cur_file_writer = None

    def close(self):
        self.cur_file_writer.close()


class Writer:
    def __init__(self, path: Path, batch_size: int, total_num_lines: int, mini:bool, cur_num_lines):
        self.cur_num_lines = cur_num_lines
        self.total_num_lines = total_num_lines
        self.start_time = time.time()
        number_read_batches, last_batch_count = cur_num_lines // batch_size, cur_num_lines % batch_size                    
        self.output_tables = {table_name: Table(path, batch_size, table_name, number_read_batches, last_batch_count) for table_name in (MINIMIZED_TABLE_NAMES if mini else TABLE_NAMES)}
        

    def write(self, json_object: Dict[str, Any]):        
        self.cur_num_lines += 1
        # print(self.cur_num_lines)
        for key, value in json_object.items():
            # print(key, value)                    
            if len(value) > 0:
                self.output_tables[key].write(value)
        if self.cur_num_lines % 200000 == 0:
            time_elapsed = time.time() - self.start_time
            estimated_time = time_elapsed * (self.total_num_lines - self.cur_num_lines) / (200000*3600)
            print(f"{self.cur_num_lines}/{self.total_num_lines} lines written in {time_elapsed:.2f}s. "
                  f"Estimated time to completion is {estimated_time:.2f} hours.")
            self.start_time = time.time()

    def close(self):
        for v in self.output_tables.values():
            # print(v.cur_file_writer)
            # print(v.table_dir)
            v.close()


def write_data(path: Path, batch_size: int, total_num_lines: int, outout_queue: Queue, mini: bool, pre_read_lines):    
    writer = Writer(path, batch_size, total_num_lines, mini, pre_read_lines)    
    path_to_count = os.path.join(path, 'readObjCount.txt')

    while True:
        json_object = outout_queue.get()
        # print(json_object)
        if json_object is None:
            break
        writer.write(json_object)    
        pre_read_lines += 1        
        with open(path_to_count, 'w') as file:
            file.write((str)(pre_read_lines))
    writer.close()
