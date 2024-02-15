import fabio
from pyFAI import load
from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
from ewokscore import Task
from ewoks import convert_graph, execute_graph
from pathlib import Path

AzimuthalIntegrator.setup_sparse_integrator

class MakeAI(
    Task,
    input_names=["poni_file"],
    output_names=["ai"],
):
    def run(self):
        ai = load(self.inputs.poni_file)
        ai.setup_sparse_integrator(shape=ai.detector.shape, npt=2000)
        self.outputs.ai = ai

class OpenData(
    Task,
    input_names=["filename", "ai"],
    output_names=["data", "ai", "filename_out"],
):
    def run(self):
        self.outputs.data = fabio.open(self.inputs.filename).data
        self.outputs.ai = self.inputs.ai
        self.outputs.filename_out = self.inputs.filename.replace(".edf", "_1d.dat")

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

def generate_subworkflow():
        node_open = {
            "id" : f"node_open", 
            "task_type" : "class", 
            "task_identifier" : "int_subworkflows.OpenData",
        }
        node_integrate = {"id" : f"node_integrate", "task_type" : "class", "task_identifier" : "int_subworkflows.IntegrateAndSave"}

        link_to_integrate = {
            "source" : f"node_open",
            "target" : f"node_integrate",
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
                              {"source_output" : "data", "target_input" : "data"},
                              {"source_output" : "filename_out", "target_input" : "filename_out"},
            ]
        }

        subgraph = {
            "graph" : {"id" : f"subgraph"},
            "nodes" : [node_open, node_integrate],
            "links" : [link_to_integrate],
        }

        convert_graph(
            subgraph,
            "_subworkflow_open_integrate.json"
        )





class ExecuteSubWorkflowDask(
    Task,
    input_names=["filename", "ai"],
):
    def run(self):
        execute_graph(
            graph="_subworkflow_open_integrate.json",
            engine="dask",
            inputs=[
                {"name" : "filename", "value" : self.inputs.filename, "id" : "node_open"},
                {"name" : "ai", "value" : self.inputs.ai, "id" : "node_open"},
            ],
        )





def execute_global_workflow(list_filenames:list, poni_file:str):
    nodes = []
    links = []

    node_makeai = {"id" : f"node_makeai", "task_type" : "class", "task_identifier" : "int_subworkflows.MakeAI"}
    nodes.append(node_makeai)
    for index, filename in enumerate(list_filenames):
        node_sub_workflow = {
            "id" : f"node_{index:04}", 
            "task_type" : "class", 
            "task_identifier" : "int_subworkflows.ExecuteSubWorkflowDask",
            "default_inputs":[{"name" : "filename", "value" : str(filename)}]
        }
        link = {
            "source" : f"node_makeai",
            "target" : f"node_{index:04}", 
            "data_mapping" : [{"source_output" : "ai", "target_input" : "ai"},
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

    execute_graph(
        graph=global_graph,
        engine="ppf",
        inputs=[
            {"name" : "poni_file", "value" : poni_file, "id" : "node_makeai"},
        ],
    )

if __name__ == "__main__":
    execute_global_workflow(
        list_filenames=[item for item in Path("edf_data").glob("*0000*.edf")],
        poni_file="lab6.poni",
    )