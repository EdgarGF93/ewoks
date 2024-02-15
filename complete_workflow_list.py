import fabio
from pyFAI import load
from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
from ewokscore import Task
from ewoks import convert_graph, execute_graph
from pathlib import Path
import time

class MakeAI(
    Task,
    input_names=["poni_file"],
    output_names=["ai"],
):
    def run(self):
        ai = load(self.inputs.poni_file)
        ai.setup_sparse_integrator(shape=ai.detector.shape, npt=2000)
        self.outputs.ai = ai

class ProxyPoni(
    Task,
    input_names=["poni"],
    output_names=["poni"],
):
    def run(self):
        self.outputs.poni = self.inputs.poni

# class OpenData(
#     Task,
#     input_names=["filename_list"],
#     optional_input_names=["ai", "poni", "index"],
#     output_names=["ai", "filename_out", "data", "index", "filename_list", "repeat"],
# ):
#     def run(self):
#         self.outputs.filename_list = self.inputs.filename_list

#         if self.missing_inputs.ai:
#             ai = load(self.inputs.poni)
#             ai.setup_sparse_integrator(shape=ai.detector.shape, npt=2000)
#         else:
#             ai = self.inputs.ai
#         self.outputs.ai = ai

#         if self.missing_inputs.index:
#             index = 0
#         else:
#             index = self.inputs.index

#         filename = self.inputs.filename_list[index]
#         self.outputs.data = fabio.open(filename).data
#         self.outputs.filename_out = filename.replace(".edf", "_1d.dat")

#         index += 1
#         if index == len(self.inputs.filename_list):
#             self.outputs.repeat = False
#         else:
#             self.outputs.repeat = True
#         self.outputs.index = index

class IntegrateAndSave(
    Task,
    input_names=["data", "ai", "filename_out"],
):
    def run(self):
        res1d = self.inputs.ai.integrate1d(
            data=self.inputs.data,
            npt=2000,
            filename=self.inputs.filename_out,
            method=("bbox", "csr", "cython"),
        )



class OpenIntegrateAndSave(
    Task,
    input_names=["filename_list"],
    optional_input_names=["ai", "poni", "index"],
    output_names=["ai", "data", "index", "filename_list", "repeat"],
):
    def run(self):
        self.outputs.filename_list = self.inputs.filename_list

        if self.missing_inputs.ai:
            ai = load(self.inputs.poni)
            ai.setup_sparse_integrator(shape=ai.detector.shape, npt=2000)
        else:
            ai = self.inputs.ai
        self.outputs.ai = ai

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index

        filename = self.inputs.filename_list[index]
        filename_out = filename.replace(".edf", "_1d.dat")
        res1d = ai.integrate1d(
            data=fabio.open(filename).data,
            npt=2000,
            filename=filename_out,
            method=("bbox", "csr", "cython"),
        )

        index += 1
        if index == len(self.inputs.filename_list):
            self.outputs.repeat = False
        else:
            self.outputs.repeat = True
        self.outputs.index = index






class OpenAI(
    Task,
    input_names=["filename_list", "poni"],
    output_names=["ai", "filename_list"],
):
    def run(self):
        ai = load(self.inputs.poni)
        ai.setup_sparse_integrator(shape=ai.detector.shape, npt=2000)
        self.outputs.ai = ai
        self.outputs.filename_list = self.inputs.filename_list

class SplitFiles(
    Task,
    input_names=["filename_list", "ai"],
    optional_input_names=["index"],
    output_names=["ai", "filename", "index", "filename_list", "repeat"],
):
    def run(self):
        self.outputs.filename_list = self.inputs.filename_list
        self.outputs.ai = self.inputs.ai

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index

        self.outputs.filename = self.inputs.filename_list[index]

        index += 1
        if index == len(self.inputs.filename_list):
            self.outputs.repeat = False
        else:
            self.outputs.repeat = True
        self.outputs.index = index

class OpenData(
    Task,
    input_names=["ai", "filename"],
    output_names=["ai", "data", "filename_out"],
):
    def run(self):
        self.outputs.ai = self.inputs.ai
        self.outputs.data = fabio.open(self.inputs.filename).data
        self.outputs.filename_out = self.inputs.filename.replace(".edf", "_1d.dat")

class Integrate(
    Task,
    input_names=["ai", "data", "filename_out"],
):
    def run(self):
        res1d = self.inputs.ai.integrate1d(
            data=self.inputs.data,
            npt=2000,
            filename=self.inputs.filename_out,
            method=("bbox", "csr", "cython"),
        )


def generate_subworkflow():
        node_open = {
            "id" : f"node_open", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.OpenData",
        }
        node_integrate = {"id" : f"node_integrate", "task_type" : "class", "task_identifier" : "complete_workflow_list.IntegrateAndSave"}

        link_to_integrate = {
            "source" : f"node_open",
            "target" : f"node_integrate",
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "data", "target_input" : "data"},
                              {"source_output" : "filename_out", "target_input" : "filename_out"},
            ]
        }

        link_self = {
            "source" : f"node_open",
            "target" : f"node_open",
            "data_mapping" : [{"source_output" : "filename_list", "target_input" : "filename_list"},
                              {"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "index", "target_input" : "index"},
            ],
            "conditions" : [{"source_output": "repeat", "value": True}],
        }



        subgraph = {
            "graph" : {"id" : f"subgraph"},
            "nodes" : [node_open, node_integrate],
            "links" : [link_to_integrate, link_self],
        }

        convert_graph(
            subgraph,
            "subworkflow_open_integrate.json"
        )




def generate_subworkflow2():
        node_open_integrate = {
            "id" : f"node_open_integrate", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.OpenIntegrateAndSave",
        }

        link_self = {
            "source" : f"node_open_integrate",
            "target" : f"node_open_integrate",
            "data_mapping" : [{"source_output" : "filename_list", "target_input" : "filename_list"},
                              {"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "index", "target_input" : "index"},
            ],
            "conditions" : [{"source_output": "repeat", "value": True}],
        }

        subgraph = {
            "graph" : {"id" : f"subgraph"},
            "nodes" : [node_open_integrate],
            "links" : [link_self],
        }

        convert_graph(
            subgraph,
            "subworkflow_open_integrate_2.json"
        )


def generate_subworkflow3():
        node_openai = {
            "id" : f"node_openai", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.OpenAI",
        }

        node_split = {
            "id" : f"node_split", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.SplitFiles",
        }

        node_opendata = {
            "id" : f"node_opendata", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.OpenData",
        }

        node_integrate = {
            "id" : f"node_integrate", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.Integrate",
        }

        link_1 = {
            "source" : f"node_openai",
            "target" : f"node_split",
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "filename_list", "target_input" : "filename_list"},
            ],
        }

        link_2 = {
            "source" : f"node_split",
            "target" : f"node_opendata",
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "filename", "target_input" : "filename"},
            ],
        }

        link_self = {
            "source" : f"node_split",
            "target" : f"node_split",
            "data_mapping" : [{"source_output" : "filename_list", "target_input" : "filename_list"},
                              {"source_output" : "index", "target_input" : "index"},
                              {"source_output" : "ai", "target_input" : "ai"},
            ],
            "conditions" : [{"source_output": "repeat", "value": True}],
        }

        link_3 = {
            "source" : f"node_opendata",
            "target" : f"node_integrate",
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "data", "target_input" : "data"},
                              {"source_output" : "filename_out", "target_input" : "filename_out"},
            ],
        }

        subgraph = {
            "graph" : {"id" : f"subgraph"},
            "nodes" : [
                node_openai,
                node_split,
                node_opendata,
                node_integrate,
            ],
            "links" : [
                link_1,
                link_2,
                link_self,
                link_3,
            ],
        }

        convert_graph(
            subgraph,
            "subworkflow_open_integrate_3.json"
        )



class ExecuteSubWorkflowPPF(
    Task,
    input_names=["filename_list", "poni"],
):
    def run(self):
        execute_graph(
            graph="subworkflow_open_integrate.json",
            engine="ppf",
            inputs=[
                {"name" : "filename_list", "value" : self.inputs.filename_list, "id" : "node_open"},
                {"name" : "poni", "value" : self.inputs.poni, "id" : "node_open"},
            ],
        )


class ExecuteSubWorkflowPPF2(
    Task,
    input_names=["filename_list", "poni"],
):
    def run(self):
        execute_graph(
            graph="subworkflow_open_integrate_2.json",
            engine="ppf",
            inputs=[
                {"name" : "filename_list", "value" : self.inputs.filename_list, "id" : "node_open_integrate"},
                {"name" : "poni", "value" : self.inputs.poni, "id" : "node_open_integrate"},
            ],
        )

class ExecuteSubWorkflowPPF3(
    Task,
    input_names=["filename_list", "poni"],
):
    def run(self):
        execute_graph(
            graph="subworkflow_open_integrate_3.json",
            engine="ppf",
            inputs=[
                {"name" : "filename_list", "value" : self.inputs.filename_list, "id" : "node_openai"},
                {"name" : "poni", "value" : self.inputs.poni, "id" : "node_openai"},
            ],
        )

def execute_global_workflow(list_filenames:list, poni:str):
    nodes = []
    links = []
    node_poni = {"id" : "node_poni", "task_type" : "class", "task_identifier" : "complete_workflow_list.ProxyPoni"}
    nodes.append(node_poni)

    for index in range(1):
        chunk_filenames = list_filenames

        node_sub_workflow = {
            "id" : f"node_{index:04}", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.ExecuteSubWorkflowPPF",
            "default_inputs":[{"name" : "filename_list", "value" : chunk_filenames}]
        }
        link = {
            "source" : f"node_poni",
            "target" : f"node_{index:04}", 
            "data_mapping" : [{"source_output" : "poni", "target_input" : "poni"},
            ]
        }
        nodes.append(node_sub_workflow)
        links.append(link)

    global_graph = {
        "graph" : {"id" : f"global_graph"},
        "nodes" : nodes,
        "links" : links,
    }

    convert_graph(
        global_graph,
        "global_workflow.json",
    )
    print("converted!")

    execute_graph(
        graph=global_graph,
        engine="ppf",
        inputs=[
            {"name" : "poni", "value" : poni, "id" : "node_poni"},
        ],
    )







def execute_global_workflow_2(list_filenames:list, poni:str):
    nodes = []
    links = []
    node_poni = {"id" : "node_poni", "task_type" : "class", "task_identifier" : "complete_workflow_list.ProxyPoni"}
    nodes.append(node_poni)

    for index in range(1):
        chunk_filenames = list_filenames

        node_sub_workflow = {
            "id" : f"node_{index:04}", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.ExecuteSubWorkflowPPF2",
            "default_inputs":[{"name" : "filename_list", "value" : chunk_filenames}]
        }
        link = {
            "source" : f"node_poni",
            "target" : f"node_{index:04}", 
            "data_mapping" : [{"source_output" : "poni", "target_input" : "poni"},
            ]
        }
        nodes.append(node_sub_workflow)
        links.append(link)

    global_graph = {
        "graph" : {"id" : f"global_graph"},
        "nodes" : nodes,
        "links" : links,
    }

    convert_graph(
        global_graph,
        "global_workflow2.json",
    )

    execute_graph(
        graph=global_graph,
        engine="ppf",
        inputs=[
            {"name" : "poni", "value" : poni, "id" : "node_poni"},
        ],
    )



def execute_global_workflow_3(list_filenames:list, poni:str):
    nodes = []
    links = []
    node_poni = {"id" : "node_poni", "task_type" : "class", "task_identifier" : "complete_workflow_list.ProxyPoni"}
    nodes.append(node_poni)

    for index in range(1):
        chunk_filenames = list_filenames

        node_sub_workflow = {
            "id" : f"node_{index:04}", 
            "task_type" : "class", 
            "task_identifier" : "complete_workflow_list.ExecuteSubWorkflowPPF3",
            "default_inputs":[{"name" : "filename_list", "value" : chunk_filenames}]
        }
        link = {
            "source" : f"node_poni",
            "target" : f"node_{index:04}", 
            "data_mapping" : [{"source_output" : "poni", "target_input" : "poni"},
            ]
        }
        nodes.append(node_sub_workflow)
        links.append(link)

    global_graph = {
        "graph" : {"id" : f"global_graph"},
        "nodes" : nodes,
        "links" : links,
    }

    convert_graph(
        global_graph,
        "global_workflow3.json",
    )

    execute_graph(
        graph=global_graph,
        engine="ppf",
        inputs=[
            {"name" : "poni", "value" : poni, "id" : "node_poni"},
        ],
    )







if __name__ == "__main__":
    # generate_subworkflow3()
    st = time.perf_counter()
    execute_global_workflow_3(
        list_filenames=[str(item) for item in Path("edf_data").glob("*000*.edf")],
        poni="lab6.poni",
    )
    print(f"Benchmark: {time.perf_counter() - st:.2f} s")