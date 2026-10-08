import os
import dispy
import time
import threading
import struct
import sys
import re
import logging

# ---- terminal styling (display only; sorting logic is unchanged) ----
_TTY = sys.stdout.isatty()
def _c(code, s): return f"\033[{code}m{s}\033[0m" if _TTY else str(s)
def dim(s): return _c("2", s)
def bold(s): return _c("1", s)
def green(s): return _c("32", s)
def red(s): return _c("31", s)
def accent(s): return _c("38;2;250;70;22", s)  # gator orange
RULE = dim("  " + "─" * 52)

def short(path):
    """sorted_job_5.bin -> 'job 05', merged_step_12.tmp -> 'step 12'"""
    b = os.path.basename(path)
    m = re.match(r"sorted_job_(\d+)\.bin$", b)
    if m: return f"job {int(m.group(1)):02d}"
    m = re.match(r"merged_step_(\d+)\.tmp$", b)
    if m: return f"step {int(m.group(1)):02d}"
    return b

def row(label, value):
    print(f"  {dim(f'{label:<9}')} {value}")

# Remote worker function passed to worker nodes
def compute_sort_chunk(path, job_id):
    import socket
    import os
    import struct

    def bubble_sort(data):
        n = len(data)
        for i in range(n):
            for j in range(0, n - i - 1):
                if data[j] > data[j + 1]:
                    data[j], data[j + 1] =
        return data

    try:
        if not os.path.exists(path):
            return (socket.gethostname(), job_id, f"error: worker cannot see file at {path}")

        # open input file in read binary mode
        with open(path, "rb") as f:
            raw_data = f.read() # read the raw byte data

        # unpack into 4-byte integers
        num_elements = len(raw_data) // 4
        nums = list(struct.unpack(f'{num_elements}i', raw_data))

        # perform the sort using your original bubble sort logic
        sorted_data = bubble_sort(nums)

        out_dir = "/mnt/nfsshare/data/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # path for the sorted binary outpu
        out_path = os.path.join(out_dir, f"sorted_job_{job_id}.bin")
        # write sorted list back to the fi
        with open(out_path, "wb") as f:
            f.write(struct.pack(f'{len(sor))

        os.remove(path)

        return (socket.gethostname(), job_

    except Exception as e:
        return (socket.gethostname(), job_id, f"python error: {str(e)}")

# Master node logic
class ParallelManager:
    def __init__(self, output_path, total_expected):
        self.output_path = output_path
        self.total_expected = total_expected
        self.ready_files = []
        self.lock = threading.Lock()
        self.merge_count = 0
        self.completed_jobs = 0

    def merge_two_files(self, file1, file2, is_final=False):
        self.merge_count += 1
        merged_name = f"merged_step_{self.merge_count}.tmp"

        if is_final:
            merged_path = self.output_path
            mode = 'w' # final merge should be ascii
        else:
            merged_path = os.path.join("/mnt/nfsshare/data/", merged_name)
            mode = 'wb' # intermediate mer

        if is_final:
            print(f"  {accent('★')}  {bold('final')}      {short(file1)} + {short(file2)} {dim('→')} {os.path.basename(merged_path)}")
        else:
            print(f"  {accent('⇢')}  merge{dim(short(file1) + ' + ' + short(file2))}")

        with open(file1, 'rb') as f1, openmerged_path, mode) as out:
            def get_val(f):
                chunk = f.read(4) #
                return struct.unpack('i', chunk)[0] if chunk else None

            val1 = get_val(f1)
            val2 = get_val(f2)

            while val1 is not None and val
                if val1 <= val2:
                    self._write_val(out, v
                    val1 = get_val(f1)
                else:
                    self._write_val(out, val2, is_final)
                    val2 = get_val(f2)

            while val1 is not None:
                self._write_val(out, val1, is_final)
                val1 = get_val(f1)

            while val2 is not None:
                self._write_val(out, val2, is_final)
                val2 = get_val(f2)

        os.remove(file1)
        os.remove(file2)
        return merged_path

    def _write_val(self, f, val, is_final)
        if is_final:
            f.write(f"{val}\n")
        else:
            f.write(struct.pack('i', val))

    def job_callback(self, job):
        if job.status == dispy.DispyJob.Finished:
            host, jid, result = job.result
            if "error" in str(result):
                print(f"  {red('✗')}  chun:')} {result}")
            else:
                self.completed_jobs += 1
                print(f"  {green('✓')}  chunk {jid:02d}   {dim('sorted on ' + host.split('.')[0])}")

                with self.lock:
                    self.ready_files.appen
                    # merge binary chunks as they become available
                    while len(self.ready_f
                        if self.completed_jobs == self.total_expected and len(self.ready_files) == 2:
                            break

                        f1 = self.ready_fi
                        f2 = self.ready_files.pop(0)
                        new_temp = self.meinal=False)
                        self.ready_files.append(new_temp)

def run_distributed_sort():
    lines_limit = 150000
    total_chunks = 100
    chunk_limit = lines_limit // total_chu
    nodes = ['192.168.0.10', '192.168.0.20', '192.168.0.30',  '192.168.0.40']
    master_ip = '192.168.1.190'

    source = "/mnt/usb/data2.set"
    data_dir = "/mnt/nfsshare/data/"

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    manager = ParallelManager("/mnt/nfsshare/final_output.txt", total_chunks)
    cluster = dispy.JobCluster(compute_sort_chunk, nodes=nodes,
                               job_status==master_ip,
                               loglevel=logging.WARNING)

    print()
    print(f"  {bold('bin-opt multinode sorter')}")
    print(RULE)
    row("input", f"{os.path.basename(source)}  {dim(f'(first {lines_limit:,} numbers)')}")
    row("chunks", f"{total_chunks} × {chunk_limit:,} numbers  {dim('packed as 4-byte ints')}")
    row("workers", ", ".join(nodes))
    print(RULE)
    print(f"  {dim('streaming input → binary chunks → workers …')}")
    print()
    start_time = time.time()

    current_chunk = []
    job_id = 0

     # read input data and convert to binary chunks on the fly
    with open(source, "r") as f:
        for line in f:
            val = line.strip()
            if val:
                current_chunk.append(int(val))

            # once a chunk is full, create a binary file and submit job immediately
            if len(current_chunk) >= chunk_limit:
                chunk_path = os.path.join(data_dir, f"input_{job_id}.bin")
                # binary: convert the ascii subset to a binary chunk file
                with open(chunk_path, "wb") as wb:
                    wb.write(struct.pack(f'{len(current_chunk)}i', *current_chunk)) # binary: pack list to bytes

                cluster.submit(chunk_path, job_id) # master sends jobs as soon as chunks are ready
                current_chunk = []
                job_id += 1
                if job_id >= total_chunks: break

    # handle any remaining numbers if the file didn't perfectly divide
    if current_chunk and job_id < total_chunks:
        chunk_path = os.path.join(data_dir, f"input_{job_id}.bin")
        # convert remaining data to binary
        with open(chunk_path, "wb") as wb:
            wb.write(struct.pack(f'{len(current_chunk)}i', *current_chunk))
        cluster.submit(chunk_path, job_id)

    cluster.wait()

    # wait for all worker jobs to finish processing
    while manager.completed_jobs < total_chunks:
        time.sleep(1)

    # final pass merge the remaining binary files into the final ascii output
    while len(manager.ready_files) > 1:
        with manager.lock:
            f1 = manager.ready_files.pop(0)
            f2 = manager.ready_files.pop(0)
            # if this is the last pair, set is_final to true for ascii output
            is_final = (len(manager.ready_files) == 0)
            manager.merge_two_files(f1, f2, is_final=is_final)

    cluster.close()
    elapsed = time.time() - start_time
    with open(manager.output_path) as f:
        n_sorted = sum(1 for _ in f)
    print()
    print(RULE)
    print(f"  {green('✔')}  {bold(f'{n_sorted:,} numbers sorted')}")
    row("chunks", f"{manager.completed_jobs} / {total_chunks} returned")
    row("merges", f"{manager.merge_count}  {dim('binary, as jobs finished')}")
    row("time", bold(f"{elapsed:.2f} s"))
    row("output", os.path.relpath(manager.output_path))
    print()

if __name__ == "__main__":
    run_distributed_sort()
