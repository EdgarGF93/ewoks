
import numpy as np
import time
import json
import matplotlib.pyplot as plt
from tasks_config_h5 import execute_global_workflow
from pathlib import Path

def benchmark_execution(
    h5_file,
    scan_number,
    detector_name,
    nfiles,
    config,
    slurm,
):
    # if slurm:
    #     chunks = np.linspace(int(nfiles / 20), int(nfiles), 20)
    # else:
    #     chunks = np.linspace(int(nfiles / 10), int(nfiles), 10)
    #chunks = [20,50,60,80,100,150,200,300,500,1000]
    return
    chunks = [10,20,30,40,50,60,70,80,90,100,200]
    y = []

    for chunk_size in chunks:

        st = time.perf_counter()
        execute_global_workflow(
            h5_file=h5_file,
            scan_number=scan_number,
            detector_name=detector_name,
            nfiles = NFILES,
            chunk_size=chunk_size,
            config=config,
            slurm=slurm,
        )
        ft = time.perf_counter() - st
        y.append(ft)

        # # remove files
        # for file_dat in Path(path_to_find).glob("eiger_*.dat"):
        #     file_dat.unlink()    

    plt.plot(chunks, np.array(y), marker='o', ls='--')
    plt.xlabel("Chunk size")
    plt.ylabel(f"Time to integrate {str(int(nfiles))} frames")
    with open(config) as fp:
        config_dict = json.load(fp)
    plt.title(str(config_dict["method"]))
    if slurm:
        title = f"benchmark_chunks_{str(str(config_dict['method']))}_{str(nfiles)}_slurm_3workers.png"
    else:
        title = f"benchmark_chunks_{str(str(config_dict['method']))}_{str(nfiles)}_local.png"
    plt.savefig(title)
    plt.close()
    np.savetxt(fname=title.replace(".png", ".dat"), X=y)

if __name__ == "__main__":
    # PATH_UNIX = "/home/esrf/edgar1993a/work/ewoks/edf_data"
    # PATH_LOCAL = "/users/edgar1993a/work/ewoks_parallel/edf_data"    

    H5 = "/data/bm28/inhouse/Edgar/data_ewoks/RAW_DATA/DL077_g3.h5"
    SCAN_NUMBER = "1.1"
    DETECTOR_NAME = "eiger"

    NFILES = 10
    CHUNK_SIZE = 10
    CONFIG = "eiger_config.json"
    SLURM = False
    BENCHMARK = False

    if BENCHMARK:
        benchmark_execution(
            h5_file=H5,
            scan_number=SCAN_NUMBER,
            detector_name=DETECTOR_NAME,
            nfiles = NFILES,
            config=CONFIG,
            slurm=SLURM,
        )
    else:
        st = time.perf_counter()
        execute_global_workflow(
            h5_file=H5,
            scan_number=SCAN_NUMBER,
            detector_name=DETECTOR_NAME,
            nfiles = NFILES,
            chunk_size=CHUNK_SIZE,
            config=CONFIG,
            slurm=SLURM,
        )
        ft = time.perf_counter() - st
        print(ft)
