
from ewokscore import Task
from ewoks import execute_graph, convert_graph
import os
import fabio
from pyFAI import load
from pathlib import Path
from itertools import islice
import numpy as np
import time
import matplotlib.pyplot as plt
from ewoksjob.client import submit
from pyFAI.method_registry import IntegrationMethod
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


class OpenAI(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "poni", "npt", "method"],
    output_names=["ai", "npt", "method", "path_to_find", "pattern", "chunk_range"],
):
    def run(self):
        ai = load(self.inputs.poni)
        ai.setup_sparse_integrator(shape=ai.detector.shape, npt=self.inputs.npt)
        self.outputs.ai = ai
        self.outputs.path_to_find = self.inputs.path_to_find
        self.outputs.chunk_range = self.inputs.chunk_range
        self.outputs.pattern = self.inputs.pattern
        self.outputs.npt = self.inputs.npt
        self.outputs.method = self.inputs.method

class OpenIntegrateSave(
     Task,
     input_names=["ai", "path_to_find", "chunk_range", "pattern", "npt", "method"],
):
     def run(self):
          path = Path(self.inputs.path_to_find)
          pattern = self.inputs.pattern
          chunk_range = [int(_) for _ in self.inputs.chunk_range]
          for filename in islice(path.glob(pattern), chunk_range[0], chunk_range[1]):
               filename = str(filename)
               data = fabio.open(filename).data
               filename_out = filename.replace(".edf", "_1d.dat")
               from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
               ll = IntegrationMethod.select_method(dim=1, algo="csr", split="bbox")
               with open("methods.txt", "w") as f:
                   f.write(str(ll))

               res1d = self.inputs.ai.integrate1d(
                    data=data,
                    npt=self.inputs.npt,
                    filename=filename_out,
                    method=self.inputs.method,
                )
               
class SplitList(
    Task,
    input_names=["path_to_find", "nfiles", "pattern", "poni", "npt", "method", "chunk_size"],
    optional_input_names=["index"],
    output_names=[
        "path_to_find", 
        "nfiles",
        "pattern",
        "poni", 
        "npt",
        "method",        
        "chunk_size",

        "chunk_range",
        "index", 
        "repeat",
        ],
):
    def run(self):
        self.outputs.path_to_find = self.inputs.path_to_find
        self.outputs.nfiles = self.inputs.nfiles
        self.outputs.pattern = self.inputs.pattern     
        self.outputs.poni = self.inputs.poni
        self.outputs.npt = self.inputs.npt
        self.outputs.method = self.inputs.method
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
    node_dummy = {"id" : "node_dummy", "task_type" : "class", "task_identifier" : "tasks_slurm_alternative.Write"}
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



def get_subworkflow(path_to_find, chunk_range, pattern, poni, npt, method):
    # Return the graph with the inputs, only need to be executed
    node_openai = {
        "id" : f"node_openai", 
        "task_type" : "class", 
        "task_identifier" : "tasks_slurm_alternative.OpenAI",
        "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                            {"name" : "chunk_range", "value" : chunk_range},
                            {"name" : "pattern", "value" : pattern},
                            {"name" : "poni", "value" : poni},
                            {"name" : "npt", "value" : npt},
                            {"name" : "method", "value" : method},
                            ]
    }

    node_openintegratesave = {
        "id" : f"node_openintegratesave", 
        "task_type" : "class", 
        "task_identifier" : "tasks_slurm_alternative.OpenIntegrateSave",
    }

    link_1 = {
        "source" : f"node_openai",
        "target" : f"node_openintegratesave",
        "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                            {"source_output" : "path_to_find", "target_input" : "path_to_find"},
                            {"source_output" : "chunk_range", "target_input" : "chunk_range"},
                            {"source_output" : "pattern", "target_input" : "pattern"},
                            {"source_output" : "npt", "target_input" : "npt"},
                            {"source_output" : "method", "target_input" : "method"},
        ],
    }

    subgraph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [
            node_openai,
            node_openintegratesave,
        ],
        "links" : [
            link_1,
        ],
    }
    return subgraph

def get_global_workflow(path_to_find, pattern, nfiles, chunk_size, poni, npt, method, slurm):
    node_split = {
         "id" : "node_split", 
         "task_type" : "class", 
         "task_identifier" : "tasks_slurm_alternative.SplitList",
         "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                             {"name" : "pattern", "value" : pattern},
                             {"name" : "nfiles", "value" : nfiles},
                             {"name" : "chunk_size", "value" : chunk_size},                             
                             {"name" : "poni", "value" : poni},
                             {"name" : "npt", "value" : npt},
                             {"name" : "method", "value" : method},
                             ]
    }
    
    if slurm:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_slurm_alternative.ExecuteSubWorkflowSLURM"}
    else:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_slurm_alternative.ExecuteSubWorkflow"}
        

    link_self = {
        "source" : f"node_split",
        "target" : f"node_split",
        "data_mapping" : [{"source_output" : "path_to_find", "target_input" : "path_to_find"},
                            {"source_output" : "nfiles", "target_input" : "nfiles"},
                            {"source_output" : "pattern", "target_input" : "pattern"},
                            {"source_output" : "poni", "target_input" : "poni"},
                            {"source_output" : "npt", "target_input" : "npt"},
                            {"source_output" : "method", "target_input" : "method"},
                            {"source_output" : "chunk_size", "target_input" : "chunk_size"},
                            {"source_output" : "index", "target_input" : "index"},
        ],
        "conditions" : [{"source_output": "repeat", "value": True}],
    }


    link_1 = {
            "source" : f"node_split",
            "target" : f"node_subworkflow",
            "data_mapping" : [{"source_output" : "path_to_find", "target_input" : "path_to_find"},
                              {"source_output" : "chunk_range", "target_input" : "chunk_range"},
                              {"source_output" : "pattern", "target_input" : "pattern"},
                              {"source_output" : "poni", "target_input" : "poni"},
                              {"source_output" : "npt", "target_input" : "npt"},
                              {"source_output" : "method", "target_input" : "method"},
            ],
        }
    


    graph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [node_split, node_subworkflow],
        "links" : [link_1, link_self],
    }
    return graph


def generate_god_workflow(path_to_find, pattern, nfiles, chunk_size,  poni='', npt=1000, method=("bbox", "csr", "cython"), execute_slurm=True):
    node_god = {
        "id" : "node_god", 
        "task_type" : "class", 
        "task_identifier" : "tasks_slurm_alternative.ExecuteGlobalWorkflow",
        "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                            {"name" : "pattern", "value" : pattern},
                            {"name" : "nfiles", "value" : nfiles},
                            {"name" : "chunk_size", "value" : chunk_size},                            
                            {"name" : "poni", "value" : poni},
                            {"name" : "npt", "value" : npt},
                            {"name" : "method", "value" : method},
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
    input_names=["path_to_find", "pattern", "nfiles", "chunk_size", "poni", "npt", "method", "slurm"],
):
    def run(self):
        # Execute the global workflow using PPF engine

        global_graph = get_global_workflow(
            path_to_find=self.inputs.path_to_find,
            pattern=self.inputs.pattern,
            nfiles=self.inputs.nfiles,
            chunk_size=self.inputs.chunk_size,            
            poni=self.inputs.poni,
            npt=self.inputs.npt,
            method=self.inputs.method,
            slurm=self.inputs.slurm,
        )

        execute_graph(
            graph=global_graph,
            engine="ppf",
        )



class ExecuteSubWorkflow(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "poni", "npt", "method"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine

        sub_graph = get_subworkflow(
            path_to_find=self.inputs.path_to_find,
            chunk_range=self.inputs.chunk_range,
            pattern=self.inputs.pattern,
            poni=self.inputs.poni,
            npt=self.inputs.npt,
            method=self.inputs.method,
        )

        execute_graph(
            graph=sub_graph,
            engine="dask",
        )

import pyslurmutils
class ExecuteSubWorkflowSLURM(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "poni", "npt", "method"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine, submitted to SLURM

        sub_graph = get_subworkflow(
            path_to_find=self.inputs.path_to_find,
            chunk_range=self.inputs.chunk_range,
            pattern=self.inputs.pattern,
            poni=self.inputs.poni,
            npt=self.inputs.npt,
            method=self.inputs.method,
        )

        kwargs = {}
        kwargs["_slurm_spawn_arguments"] = {
            "pre_script": "module load cuda",
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
            # "pre_script": "module load myotherenv",
        }

        # Now we have to submit this graph to slurm
        future = submit(args=(sub_graph,), kwargs=kwargs)
        result = future.get(timeout=None)



        # name_workflow = f"subworkflow_slurm_{str(self.inputs.chunk_range)}.json".replace(" ", "_")
        # convert_graph(sub_graph, name_workflow)
        # activate_slurm_env()
        # cmd = f"ewoks submit {name_workflow}"
        # os.system(cmd)

def benchmark_execution(
    path_to_find,
    pattern,
    nfiles,
    poni,
    npt,
    method,
):
    chunks = np.linspace(int(nfiles / 10), int(nfiles), 5)
    y = []
    os.system("rm edf_data/*.dat")
    for chunk_size in chunks:
        st = time.perf_counter()
        generate_god_workflow(
            path_to_find=path_to_find,
            pattern=pattern,
            nfiles = nfiles,
            chunk_size=chunk_size,        
            poni=poni,
            npt=npt,
            method=method,
            execute_local=True,
            execute_slurm=False,
        )
        ft = time.perf_counter() - st
        y.append(ft)
        os.system("rm edf_data/*.dat")
    plt.plot(chunks, np.array(y), marker='o', ls='--')
    plt.xlabel("Chunk size")
    plt.ylabel(f"Time to integrate {str(int(nfiles))} frames")
    plt.title(str(method))
    plt.savefig(f"benchmark_chunks_{str(method)}_{str(nfiles)}.png")
    plt.close()

if __name__ == "__main__":
    PATH_UNIX = "/home/esrf/edgar1993a/work/ewoks/edf_data"
    PATH_LOCAL = "/users/edgar1993a/work/ewoks_parallel/edf_data"
    PATTERN = "*.edf"
    PONI = "data/lab6.poni"
    NPT = 2000
    METHOD = ("bbox", "csr", "cython")
    NFILES = 5
    CHUNK_SIZE = 2

    st = time.perf_counter()
    generate_god_workflow(
        path_to_find=PATH_UNIX,
        pattern=PATTERN,
        nfiles = NFILES,
        chunk_size=CHUNK_SIZE,
        poni=PONI,
        npt=NPT,
        method=METHOD,
        execute_slurm=True,
    )
    ft = time.perf_counter() - st
    print(ft)


    # benchmark_execution(
    #     path_to_find=PATH_LOCAL,
    #     pattern=PATTERN,
    #     nfiles = NFILES,
    #     poni=PONI,
    #     npt=NPT,
    #     method=METHOD,
    # )
    # x = np.array([20, 35, 65, 100, 150])
    # y = np.array([46, 34, 18.8, 28, 33.23])
    # plt.plot(x, y, marker='o', ls='--')
    # plt.xlabel("Chunk size")
    # plt.ylabel(f"Time to integrate 1000 frames")
    # plt.title("bbox_csr_cython")
    # plt.savefig(f"benchmark_chunks_bbox_csr_cython_1000_SLURM.png")
    # plt.close()
