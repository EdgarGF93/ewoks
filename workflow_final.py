from ewokscore import Task
from pyFAI import load
from ewoks import convert_graph, execute_graph
import fabio
from pathlib import Path
import time
import numpy as np
import matplotlib.pyplot as plt
import os

class OpenAI(
    Task,
    input_names=["filename_list", "poni", "npt", "method"],
    output_names=["ai", "filename_list", "npt", "method"],
):
    def run(self):
        ai = load(self.inputs.poni)
        ai.setup_sparse_integrator(shape=ai.detector.shape, npt=self.inputs.npt)
        self.outputs.ai = ai
        self.outputs.filename_list = self.inputs.filename_list
        self.outputs.npt = self.inputs.npt
        self.outputs.method = self.inputs.method

class OpenIntegrateSave(
     Task,
     input_names=["ai", "filename_list", "npt", "method"],
):
     def run(self):
          for filename in self.inputs.filename_list:
               data = fabio.open(filename).data
               filename_out = filename.replace(".edf", "_1d.dat")
               res1d = self.inputs.ai.integrate1d(
                    data=data,
                    npt=self.inputs.npt,
                    filename=filename_out,
                    method=self.inputs.method,
                )
               
class SplitList(
    Task,
    input_names=["filename_list", "poni", "npt", "method"],
    optional_input_names=["index", "chunk_size"],
    output_names=[
        "poni", 
        "chunked_list", 
        "index", 
        "filename_list", 
        "repeat", 
        "chunk_size",
        "npt",
        "method",
        ],
):
    def run(self):
        self.outputs.filename_list = self.inputs.filename_list
        self.outputs.poni = self.inputs.poni
        self.outputs.npt = self.inputs.npt
        self.outputs.method = self.inputs.method

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index

        if self.missing_inputs.chunk_size:
            chunk_size = 3
        else:
            chunk_size = self.inputs.chunk_size
        self.outputs.chunk_size = chunk_size

        chunk_range = [index * chunk_size, (index + 1) * chunk_size]

        if chunk_range[0] == len(self.inputs.filename_list):
            self.outputs.chunked_list = []
            self.outputs.repeat = False
        elif chunk_range[1] >= len(self.inputs.filename_list):
            self.outputs.chunked_list = self.inputs.filename_list[chunk_range[0]:chunk_range[1]]
            self.outputs.repeat = False
        else:
            self.outputs.chunked_list = self.inputs.filename_list[chunk_range[0]:chunk_range[1]]
            self.outputs.repeat = True
        
        self.outputs.index = index + 1

class ExecuteDask(
    Task,
    input_names=["chunked_list", "poni", "npt", "method"],
):
    def run(self):
        execute_graph(
            graph="final_subworkflow.json",
            engine="dask",
            inputs=[
                {"name" : "poni", "value" : self.inputs.poni, "id" : "node_openai"},
                {"name" : "filename_list", "value" : self.inputs.chunked_list, "id" : "node_openai"},
                {"name" : "npt", "value" : self.inputs.npt, "id" : "node_openai"},
                {"name" : "method", "value" : self.inputs.method, "id" : "node_openai"},
            ],
        )

class ExecuteDaskSLURM(
    Task,
    input_names=["chunked_list", "poni", "npt", "method"],
):
    def run(self):
        execute_graph(
            graph="final_subworkflow_slurm.json",
            engine="dask",
            inputs=[
                {"name" : "poni", "value" : self.inputs.poni, "id" : "node_openai"},
                {"name" : "filename_list", "value" : self.inputs.chunked_list, "id" : "node_openai"},
                {"name" : "npt", "value" : self.inputs.npt, "id" : "node_openai"},
                {"name" : "method", "value" : self.inputs.method, "id" : "node_openai"},
            ],
        )


def generate_subworkflow():
        node_openai = {
            "id" : f"node_openai", 
            "task_type" : "class", 
            "task_identifier" : "workflow_final.OpenAI",
        }

        node_openintegratesave = {
            "id" : f"node_openintegratesave", 
            "task_type" : "class", 
            "task_identifier" : "workflow_final.OpenIntegrateSave",
        }

        link_1 = {
            "source" : f"node_openai",
            "target" : f"node_openintegratesave",
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "filename_list", "target_input" : "filename_list"},
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

        convert_graph(
            subgraph,
            "final_subworkflow.json"
        )

def generate_global_workflow():
    node_split = {
         "id" : "node_split", 
         "task_type" : "class", 
         "task_identifier" : "workflow_final.SplitList"}
    
    node_subworkflow = {
         "id" : "node_subworkflow", 
         "task_type" : "class", 
         "task_identifier" : "workflow_final.ExecuteDask"}
    
    link_1 = {
            "source" : f"node_split",
            "target" : f"node_subworkflow",
            "data_mapping" : [{"source_output" : "chunked_list", "target_input" : "chunked_list"},
                              {"source_output" : "poni", "target_input" : "poni"},
                              {"source_output" : "npt", "target_input" : "npt"},
                              {"source_output" : "method", "target_input" : "method"},
            ],
        }
    link_self = {
        "source" : f"node_split",
        "target" : f"node_split",
        "data_mapping" : [{"source_output" : "filename_list", "target_input" : "filename_list"},
                            {"source_output" : "poni", "target_input" : "poni"},
                            {"source_output" : "index", "target_input" : "index"},
                            {"source_output" : "chunk_size", "target_input" : "chunk_size"},
                            {"source_output" : "npt", "target_input" : "npt"},
                            {"source_output" : "method", "target_input" : "method"},
        ],
        "conditions" : [{"source_output": "repeat", "value": True}],
    }

    graph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [node_split, node_subworkflow],
        "links" : [link_1, link_self],
    }

    convert_graph(
        graph,
        "final_global_workflow.json"
    )


def execute_ewoks_parallel(list_files, poni_file, chunk_size, npt, method):
    execute_graph(
        graph="final_global_workflow.json",
        engine="ppf",
        inputs=[
            {"name" : "poni", "value" : poni_file, "id" : "node_split"},
            {"name" : "filename_list", "value" : list_files, "id" : "node_split"},
            {"name" : "chunk_size", "value" : int(chunk_size), "id" : "node_split"},
            {"name" : "npt", "value" : npt, "id" : "node_split"},
            {"name" : "method", "value" : method, "id" : "node_split"}, 
        ],
    )

def benchmark_execution(list_files, poni_file, npt, method):
    NFILES = len(list_files)
    chunks = np.linspace(int(NFILES / 10), int(NFILES), 20)
    y = []
    os.system("rm edf_data/*.dat")
    for chunk_size in chunks:
        st = time.perf_counter()
        execute_ewoks_parallel(
            list_files=list_files,
            poni_file=poni_file,
            chunk_size=chunk_size,
            npt=npt,
            method=method,
        )
        ft = time.perf_counter() - st
        y.append(ft)
        os.system("rm edf_data/*.dat")
    plt.plot(chunks, np.array(y), marker='o', ls='--')
    plt.xlabel("Chunk size")
    plt.ylabel(f"Time to integrate {str(int(NFILES))} frames")
    plt.title(str(method))
    plt.savefig(f"benchmark_chunks_{str(method)}_{str(NFILES)}.png")
    plt.close()

if __name__ == "__main__":
    FILENAME_LIST = [str(item) for item in Path("edf_data").glob("*.edf")]
    PONI = "lab6.poni"
    NPT = 2000
    METHODS = [("bbox", "csr", "cython")]

    for method in METHODS:
        benchmark_execution(
            list_files= FILENAME_LIST,
            poni_file=PONI,
            npt=NPT,
            method=method,
        )


