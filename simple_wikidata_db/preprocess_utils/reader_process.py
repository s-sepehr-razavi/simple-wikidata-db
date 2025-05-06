from multiprocessing import Queue, Value
from pathlib import Path
import gzip
import bz2
import os

def count_lines(input_file: Path, logging_path, cache_path="./output/num_lines_count.txt"):
    # If the cache file exists, read and return the cached count    
    print(f"Counting number of lines in the {input_file}")
    with open(logging_path, "a") as log_file:
                log_file.write(f"Counting number of lines in the {input_file}\n")
                log_file.flush()
    cnt = 0
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            try:
                cnt = int(f.read().strip())
                print(f"Loaded line count from cache: {cnt}")
                with open(logging_path, "a") as log_file:
                    log_file.write(f"Loaded line count from cache: {cnt}\n")
                    log_file.flush()
                return cnt
            except ValueError:
                print("Cache file is corrupted. Recounting lines...")
                with open(logging_path, "a") as log_file:
                    log_file.write("Cache file is corrupted. Recounting lines...\n")
                    log_file.flush()
    
    print(f"Counting lines in {input_file} ...")
    with open(logging_path, "a") as log_file:
        log_file.write(f"Counting lines in {input_file} ...\n")
    if input_file.suffix == ".bz2":
        f = bz2.open(input_file, "r")
    elif input_file.suffix == ".gz":
        f = gzip.open(input_file, "rb")
    else:
        raise ValueError(f"The file must be either .bz2 or .gz, but got {input_file.suffix}.")

    for _ in f:
        cnt += 1        
        
        # Save the result to cache
    with open(cache_path, 'w') as f:
        f.write(str(cnt))

    print(f"Counted {cnt} lines and cached the result.")
    with open(logging_path, "a") as log_file:
        log_file.write(f"Counted {cnt} lines and cached the result.\n")
        log_file.flush()
    return cnt

def read_data(input_file: Path, num_lines_read: Value, max_lines_to_read: int, work_queue: Queue, pre_read_lines, stop_flag):
    """
    Reads the data from the input file and pushes it to the output queue.
    :param input_file: Path to the input file.
    :param num_lines_read: Value to store the number of lines in the input file.
    :param max_lines_to_read: Maximum number of lines to read from the input file (for testing).
    :param work_queue: Queue to push the data to.
    """
    if input_file.suffix == ".bz2":
        f = bz2.open(input_file, "r")
    elif input_file.suffix == ".gz":
        f = gzip.GzipFile(input_file, "r")
    else:
        raise ValueError(f"The file must be either .bz2 or .gz, but got {input_file.suffix}.")

    counter = 1
    num_lines = 0    
    for ln in f:
        # print(counter)
        # print(ln)
        if ln == b"[\n" or ln == b"]\n":
            continue
        if counter <= pre_read_lines: #BUG when 0 0 
            counter += 1
            continue
        if ln.endswith(b",\n"):  # all but the last element
            obj = ln[:-2]
        else:
            obj = ln
        num_lines += 1        
        work_queue.put(obj)
        if 0 < max_lines_to_read <= num_lines or stop_flag.value:
            break        
    num_lines_read.value = num_lines
    
    f.close()
    return
