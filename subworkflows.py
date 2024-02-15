from ewoks import execute_graph, convert_graph
from ewokscore import Task
from pathlib import Path
import fabio
import numpy as np
from pyFAI 

class ProxyIntegration(
    Task,
    input_names=["poni_file", "data_filename"],
    optional_input_names=["mask_file"],
    output_names=["poni_file", "image", "mask", "output_file"],
):
    # Yields the poni file, the data array, the mask array and the output file
    def run(self):
        self.outputs.poni_file = self.inputs.poni_file
        self.outputs.image = fabio.open(self.inputs.data_filename).data
        if self.missing_inputs.mask_file:
            self.outputs.mask = np.zeros(self.outputs.image.shape)
        else:
            self.outputs.mask = fabio.open(self.inputs.mask_file).data
        self.outputs.output_file = self.inputs.data_filename.replace(".edf", "_1d.dat")


class 





class ExecuteJsonPPF(
    Task,
    input_names=["json_file", "poni_file", "data_filename"],
):
    def run(self):
        execute_graph(
            graph=self.inputs.json_file,
            engine="ppf",
            inputs=[
                {"name" : "poni_file", "value" : self.inputs.poni_file, "id" : "node_proxy"},
                {"name" : "data_filename", "value" : self.inputs.data_filename, "id" : "node_proxy"},
            ],
        )

class ExecuteJsonDask(
    Task,
    input_names=["json_file", "poni_file", "data_filename"],
):
    def run(self):
        execute_graph(
            graph=self.inputs.json_file,
            engine="dask",
            inputs=[
                {"name" : "poni_file", "value" : self.inputs.poni_file, "id" : "node_proxy"},
                {"name" : "data_filename", "value" : self.inputs.data_filename, "id" : "node_proxy"},
            ],
        )







def generate_subworkflow_ppf():
    node_json = {"id" : "node_json", "task_type" : "class", "task_identifier" : "subworkflows.ExecuteJsonPPF"}

    graph = {
        "graph" : {"id" : "graph_json_ppf"},
        "nodes" : [node_json],
    }
    convert_graph(
        graph,
        "execute_ppf.json"
    )

def generate_subworkflow_dask():
    node_json = {"id" : "node_json", "task_type" : "class", "task_identifier" : "subworkflows.ExecuteJsonDask"}

    graph = {
        "graph" : {"id" : "graph_json_dask"},
        "nodes" : [node_json],
    }
    convert_graph(
        graph,
        "execute_dask.json"
    )



def subworkflow():
    node_proxy = {"id" : "node_proxy", "task_type" : "class", "task_identifier" : "subworkflows.ProxyIntegration"}
    node_config = {"id" : "node_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_integrate = {"id" : "node_integrate", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1D"}
    node_save = {"id" : "node_save", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.ascii.SaveAsciiPattern1D"}

    link_proxy_to_config = {
        "source" : "node_proxy",
        "target" : "node_config",
        "data_mapping" : [
            {"source_output" : "poni_file", "target_input" : "filename"},
        ]
    }

    link_proxy_to_integrate = {
        "source" : "node_proxy",
        "target" : "node_integrate",
        "data_mapping" : [
            {"source_output" : "image", "target_input" : "image"},
            {"source_output" : "mask", "target_input" : "mask"},
        ]
    }

    link_config_to_integrate = {
        "source" : "node_config",
        "target" : "node_integrate",
        "data_mapping" : [
            {"source_output" : "detector", "target_input" : "detector"},
            {"source_output" : "geometry", "target_input" : "geometry"},
            {"source_output" : "energy", "target_input" : "energy"},
        ]
    }

    link_integrate_to_save = {
        "source" : "node_integrate",
        "target" : "node_save",
        "data_mapping" : [
            {"source_output" : "x", "target_input" : "x"},
            {"source_output" : "y", "target_input" : "y"},
            {"source_output" : "xunits", "target_input" : "xunits"},
        ]
    }

    link_proxy_to_save = {
        "source" : "node_proxy",
        "target" : "node_save",
        "data_mapping" : [
            {"source_output" : "output_file", "target_input" : "filename"},
        ]
    }

    graph = {
        "graph" : {"id" : "graph_subworkflow"},
        "nodes" : [node_proxy, node_config, node_integrate, node_save],
        "links" : [
            link_proxy_to_config,
            link_integrate_to_save,
            link_config_to_integrate,
            link_proxy_to_integrate,
            link_proxy_to_save,
        ],
    }

    convert_graph(
        graph,
        "subworkflow.json",
    )

if __name__ == "__main__":
    generate_subworkflow_dask()