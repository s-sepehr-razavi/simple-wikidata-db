from multiprocessing import Process, Value
import multiprocessing
import shutil
import time
import os

def monitor_disk_usage(out_dir, stop_flag, maximum_memory_usage):
    with open("monitor.log", "a") as log_file:
        log_file.write("Monitor process started\n")
        log_file.flush()
        while not stop_flag.value:
            total, used, free = shutil.disk_usage(out_dir)
            used_percent = used / total
            log_file.write(f"Used: {used_percent:.2f}\n")
            print(f"Used: {used_percent:.2f}\n")
            log_file.flush()
            if used_percent > maximum_memory_usage/100:
                log_file.write("Disk usage exceeded. Stopping...\n")
                stop_flag.value = 1
            time.sleep(5)

def useless_func():
    while True:
        continue

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    out_dir = "."  # or wherever
    stop_flag = Value('i', 0)
    p = Process(target=monitor_disk_usage, args=(out_dir, stop_flag, 80))
    p.start()
    print("this") 
    useless_func()
    stop_flag.value = 1
    p.join()
