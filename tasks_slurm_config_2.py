
from ewokscore import Task
from ewoks import execute_graph, convert_graph
import os
import fabio
from pyFAI import load
from pathlib import Path
from itertools import islice
import numpy as np
import time
import json
import matplotlib.pyplot as plt
from ewoksjob.client import submit
from pyFAI.method_registry import IntegrationMethod
from pyFAI.app.integrate import process
from pyFAI.worker import Worker
from pyFAI.io import DefaultAiWriter

class Write(
    Task,
    optional_input_names=["string"],
):
    def run(self):
        if self.missing_inputs.string:
            string = "hola"
        else:
            string = self.inputs.string

        with open("write_dummy.txt", "a+") as f:
            f.write(string)

class OpenIntegrateSave(Task, input_names=["path_to_find", "chunk_range", "pattern", "config"]):
     def run(self):
        path = Path(self.inputs.path_to_find)
        pattern = self.inputs.pattern
        chunk_range = [int(_) for _ in self.inputs.chunk_range]
        list_filenames = [str(item) for item in islice(path.glob(pattern), chunk_range[0], chunk_range[1])]

        with open(self.inputs.config) as fp:
            config = json.load(fp)
        ai = load(self.inputs.config)
        ai.setup_sparse_integrator(shape=ai.detector.shape, npt=config["nbpt_rad"])

        for filename in list_filenames:
            res = ai.integrate1d(
                data=fabio.open(filename).data,
                npt=config["nbpt_rad"],
                filename=filename.replace(".edf", ".dat"),
                method=("bbox", "csr", "opencl"),
                #method=tuple(config["method"]),
                #method=("bbox", "csr", "opencl"),
            )

        # Do not work with opencl implementation, the same using the worker explicitly
        # with open(self.inputs.config) as fp:
        #     config = json.load(fp)
            
        # process(
        #       input_data=list_filenames,
        #       config=config,
        #       output=None,
        #       monitor_name=None,
        #       observer=None,
        #   )
               
class SplitList(
    Task,
    input_names=[
        "nfiles", 
        "chunk_size", 
    ],
    optional_input_names=["index"],
    output_names=[
        "nfiles",
        "chunk_size",
        "chunk_range",
        "index", 
        "repeat",
        ],
):
    def run(self):
        self.outputs.nfiles = self.inputs.nfiles
        self.outputs.chunk_size = self.inputs.chunk_size

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index
        
        chunk_range = [index * self.inputs.chunk_size, (index + 1) * self.inputs.chunk_size]

        if chunk_range[0] == self.inputs.nfiles:
            self.outputs.repeat = False
        elif chunk_range[1] >= self.inputs.nfiles:
            self.outputs.repeat = False
        else:
            self.outputs.repeat = True
        self.outputs.chunk_range = chunk_range
        self.outputs.index = index + 1


def generate_workflow_dummy(execute=True):
    node_dummy = {"id" : "node_dummy", "task_type" : "class", "task_identifier" : "tasks_slurm_config_2.Write"}
    graph = {"graph" : {"id" : "dummy_graph"}, "nodes" : [node_dummy], "links" : []}
    convert_graph(graph, "dummy_workflow.json")
    if execute:
        execute_graph(graph)

def activate_slurm_env():
    os.system("source activate_slurm_env.sh")

def activate_redis():
    # type sudo service redis-server stop if its not opening
    os.system("source activate_redis.sh")

def activate_worker():
    os.system("source activate_worker.sh")

def submit_dummy_workflow():
    os.system("source submit_dummy.sh")



def get_subworkflow(path_to_find, chunk_range, pattern, config):

    node_openintegratesave = {
        "id" : f"node_openintegratesave", 
        "task_type" : "class", 
        "task_identifier" : "tasks_slurm_config_2.OpenIntegrateSave",
        "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                            {"name" : "chunk_range", "value" : chunk_range},
                            {"name" : "pattern", "value" : pattern},
                            {"name" : "config", "value" : config},
                            ]
    }

    subgraph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [
            node_openintegratesave,
        ],
        "links" : [],
    }
    return subgraph

def get_global_workflow(path_to_find, pattern, nfiles, chunk_size, config, slurm):
    node_split = {
         "id" : "node_split", 
         "task_type" : "class", 
         "task_identifier" : "tasks_slurm_config_2.SplitList",
         "default_inputs" : [
             {"name" : "nfiles", "value" : nfiles},
             {"name" : "chunk_size", "value" : chunk_size},                             
            ]
    }
    
    if slurm:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_slurm_config_2.ExecuteSubWorkflowSLURM",
            "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                                {"name" : "pattern", "value" : pattern},
                                {"name" : "config", "value" : config},
                                ]
        }
    else:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_slurm_config_2.ExecuteSubWorkflow",
            "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                                {"name" : "pattern", "value" : pattern},
                                {"name" : "config", "value" : config},
                                ]
        }
        

    link_self = {
        "source" : f"node_split",
        "target" : f"node_split",
        "data_mapping" : [
            {"source_output" : "nfiles", "target_input" : "nfiles"},
            {"source_output" : "chunk_size", "target_input" : "chunk_size"},
            {"source_output" : "index", "target_input" : "index"},
        ],
        "conditions" : [{"source_output": "repeat", "value": True}],
    }


    link_1 = {
            "source" : f"node_split",
            "target" : f"node_subworkflow",
            "data_mapping" : [
                {"source_output" : "chunk_range", "target_input" : "chunk_range"},
            ],
        }
    
    graph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [node_split, node_subworkflow],
        "links" : [link_1, link_self],
        
    }
    return graph


def generate_god_workflow(path_to_find, pattern, nfiles, chunk_size, config, execute_slurm=True):
    node_god = {
        "id" : "node_god", 
        "task_type" : "class", 
        "task_identifier" : "tasks_slurm_config_2.ExecuteGlobalWorkflow",
        "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                            {"name" : "pattern", "value" : pattern},
                            {"name" : "nfiles", "value" : nfiles},
                            {"name" : "chunk_size", "value" : chunk_size},                            
                            {"name" : "config", "value" : config},
                            {"name" : "slurm", "value" : execute_slurm},
                            ]
    }
    graph_god = {"graph" : {"id" : "graph_god"}, "nodes" : [node_god], "links" : []}
    # convert_graph(graph_god, "god_workflow.json")
    if execute_slurm:
        activate_slurm_env()

    execute_graph(graph=graph_god, engine="dask")


class ExecuteGlobalWorkflow(
    Task,
    input_names=["path_to_find", "pattern", "nfiles", "chunk_size", "config"],
):
    def run(self):
        # Execute the global workflow using PPF engine

        global_graph = get_global_workflow(
            path_to_find=self.inputs.path_to_find,
            pattern=self.inputs.pattern,
            nfiles=self.inputs.nfiles,
            chunk_size=self.inputs.chunk_size,
            config=self.inputs.config,
            slurm=self.inputs.slurm,
        )

        execute_graph(
            graph=global_graph,
            engine="ppf",
        )



class ExecuteSubWorkflow(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "config"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine

        sub_graph = get_subworkflow(
            path_to_find=self.inputs.path_to_find,
            chunk_range=self.inputs.chunk_range,
            pattern=self.inputs.pattern,
            config=self.inputs.config,
        )

        execute_graph(
            graph=sub_graph,
            engine="dask",
        )

import pyslurmutils
class ExecuteSubWorkflowSLURM(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "config"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine, submitted to SLURM

        sub_graph = get_subworkflow(
            path_to_find=self.inputs.path_to_find,
            chunk_range=self.inputs.chunk_range,
            pattern=self.inputs.pattern,
            config=self.inputs.config,
        )

        kwargs = {}
        kwargs["_slurm_spawn_arguments"] = {
            "pre_script": "module load ewoks",
            "parameters": {
                "time_limit": 360,
                "minimum_cpus_per_node" : 14,
                # "max_cpus" : {
                # "number" : 5,
                # "set" : False,
                # "infinite" : True,
                # },
                # "gpus_per_node" : 1,
                # "current_working_directory": "/other/path/to/data",
            },
        }

        # Now we have to submit this graph to slurm
        future = submit(args=(sub_graph,), kwargs=kwargs)
        result = future.get(timeout=None)

def benchmark_execution(
    path_to_find,
    pattern,
    nfiles,
    config,
):
    chunks = np.linspace(int(nfiles / 10), int(nfiles), 10)
    y = []
    os.system("rm edf_data/*.dat")
    for chunk_size in chunks:
        st = time.perf_counter()
        generate_god_workflow(
            path_to_find=path_to_find,
            pattern=pattern,
            nfiles = nfiles,
            chunk_size=chunk_size,
            config=config,
            execute_slurm=False,
        )
        ft = time.perf_counter() - st
        y.append(ft)
        os.system("rm edf_data/*.dat")
    plt.plot(chunks, np.array(y), marker='o', ls='--')
    plt.xlabel("Chunk size")
    plt.ylabel(f"Time to integrate {str(int(nfiles))} frames")
    with open(config) as fp:
        config_dict = json.load(fp)
    plt.title(str(config_dict["method"]))
    # plt.savefig(f"benchmark_chunks_{str(str(config_dict["method"]))}_{str(nfiles)}.png")
    plt.close()

if __name__ == "__main__":
    PATH_UNIX = "/home/esrf/edgar1993a/work/ewoks/edf_data"
    PATH_LOCAL = "/users/edgar1993a/work/ewoks_parallel/edf_data"
    PATTERN = "*.edf"

    NFILES = 2
    CHUNK_SIZE = 5
    CONFIG = "ewoks_config.json"

    st = time.perf_counter()
    generate_god_workflow(
        path_to_find=PATH_LOCAL,
        pattern=PATTERN,
        nfiles = NFILES,
        chunk_size=CHUNK_SIZE,
        config=CONFIG,
        execute_slurm=False,
    )
    ft = time.perf_counter() - st
    print(ft)





    # benchmark_execution(
    #     path_to_find=PATH_LOCAL,
    #     pattern=PATTERN,
    #     nfiles=NFILES,
    #     config=CONFIG,
    # )

    # x = np.array([20, 35, 65, 100, 150])
    # y = np.array([46, 34, 18.8, 28, 33.23])
    # plt.plot(x, y, marker='o', ls='--')
    # plt.xlabel("Chunk size")
    # plt.ylabel(f"Time to integrate 1000 frames")
    # plt.title("bbox_csr_cython")
    # plt.savefig(f"benchmark_chunks_bbox_csr_cython_1000_SLURM.png")
    # plt.close()
