
import numpy as np
import time
import json
import matplotlib.pyplot as plt
from tasks_config import execute_god_workflow
from pathlib import Path

def benchmark_execution(
    path_to_find,
    pattern,
    nfiles,
    config,
    slurm,
):
    if slurm:
        chunks = np.linspace(int(nfiles / 20), int(nfiles), 20)
    else:
        chunks = np.linspace(int(nfiles / 10), int(nfiles), 10)
    y = []

    for chunk_size in chunks:

        st = time.perf_counter()
        execute_god_workflow(
            path_to_find=path_to_find,
            pattern=pattern,
            nfiles = nfiles,
            chunk_size=chunk_size,
            config=config,
            slurm=slurm,
        )
        ft = time.perf_counter() - st
        y.append(ft)

        # remove files
        for file_dat in Path(path_to_find).glob("p1m_*.dat"):
            file_dat.unlink()    

    plt.plot(chunks, np.array(y), marker='o', ls='--')
    plt.xlabel("Chunk size")
    plt.ylabel(f"Time to integrate {str(int(nfiles))} frames")
    with open(config) as fp:
        config_dict = json.load(fp)
    plt.title(str(config_dict["method"]))
    if slurm:
        title = f"benchmark_chunks_{str(str(config_dict['method']))}_{str(nfiles)}_slurm.png"
    else:
        title = f"benchmark_chunks_{str(str(config_dict['method']))}_{str(nfiles)}_local.png"
    plt.savefig(title)
    plt.close()

if __name__ == "__main__":
    # PATH_UNIX = "/home/esrf/edgar1993a/work/ewoks/edf_data"
    # PATH_LOCAL = "/users/edgar1993a/work/ewoks_parallel/edf_data"    

    PATH_DATA_INHOUSE = "/data/bm28/inhouse/Edgar/data_ewoks/P1M"
    PATTERN = "*.edf"
    NFILES = 100
    CHUNK_SIZE = 30
    CONFIG = "p1m_config_cython.json"
    #CONFIG = "ewoks_config_cython_unix.json"
    SLURM = True
    BENCHMARK = False

    if BENCHMARK:
        benchmark_execution(
            path_to_find=PATH_DATA_INHOUSE,
            pattern=PATTERN,
            nfiles=NFILES,
            config=CONFIG,
            slurm=SLURM,
        )
    else:
        st = time.perf_counter()
        execute_god_workflow(
            path_to_find=PATH_DATA_INHOUSE,
            pattern=PATTERN,
            nfiles = NFILES,
            chunk_size=CHUNK_SIZE,
            config=CONFIG,
            slurm=SLURM,
        )
        ft = time.perf_counter() - st
        print(ft)
