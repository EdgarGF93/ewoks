
from ewokscore import Task
from ewoks import execute_graph, convert_graph
import os
import json
import fabio
from pyFAI import load
from pyFAI.io

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














def generate_workflow_dummy(execute=True):
    node_dummy = {"id" : "node_dummy", "task_type" : "class", "task_identifier" : "tasks_slurm.Write"}
    graph = {"graph" : {"id" : "dummy_graph"}, "nodes" : [node_dummy], "links" : []}
    convert_graph(graph, "workflows/dummy_workflow.json")
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



def get_subworkflow(filename_list, poni, npt, method):
        # Return the graph with the inputs, only need to be executed
        node_openai = {
            "id" : f"node_openai", 
            "task_type" : "class", 
            "task_identifier" : "tasks_parallel.OpenAI",
            "default_inputs" : [{"name" : "filename_list", "value" : filename_list},
                                {"name" : "poni", "value" : poni},
                                {"name" : "npt", "value" : npt},
                                {"name" : "method", "value" : method},
                                ]
        }

        node_openintegratesave = {
            "id" : f"node_openintegratesave", 
            "task_type" : "class", 
            "task_identifier" : "tasks_parallel.OpenIntegrateSave",
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
        return subgraph


def generate_global_workflow(filename_list, poni, npt, method):
    node_split = {
         "id" : "node_split", 
         "task_type" : "class", 
         "task_identifier" : "tasks_parallel.SplitList",
         "default_inputs" : [{"name" : "filename_list", "value" : filename_list},
                             {"name" : "poni", "value" : poni},
                             {"name" : "npt", "value" : npt},
                             {"name" : "method", "value" : method},
                             ]
         }
    
    node_subworkflow = {
         "id" : "node_subworkflow", 
         "task_type" : "class", 
         "task_identifier" : "tasks_parallel.ExecuteDask"}
    
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
        "global_workflow_parallel_slurm.json"
    )


class ExecuteDaskSLURM(
    Task,
    input_names=["chunked_list", "poni", "npt", "method"],
):
    def run(self):
        with open("final_subworkflow.json") as fp:
            graph_subworkflow = json.load(fp)
        print(graph_subworkflow)
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

if __name__ == "__main__":
    with open("subworkflow_parallel.json") as fp:
        graph_subworkflow = json.load(fp)
    print(graph_subworkflow)

