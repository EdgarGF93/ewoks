from ewokscore import Task
import fabio
from ewoks import execute_graph, convert_graph
from pathlib import Path

class ProxyOpenData(
    Task,
    input_names=["edf_filename"],
    output_names=["image", "filename_out"],
):
    def run(self):
        self.outputs.image = fabio.open(self.inputs.edf_filename).data
        filename_out = str(self.inputs.edf_filename).replace(".edf", "_1d.dat")
        self.outputs.filename_out = filename_out

class ProxyOpenManyData(
    Task,
    input_names=["list_filenames"],
    optional_input_names=["index"],
    output_names=["image", "filename_out", "list_filenames", "index", "repeat"],
):
    def run(self):
        self.outputs.list_filenames = self.inputs.list_filenames

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index

        if index == len(self.inputs.list_filenames) - 1:
            self.outputs.repeat = False
        else:
            self.outputs.repeat = True

        filename = self.inputs.list_filenames[index]
        self.outputs.image = fabio.open(filename).data

        filename_out = str(self.inputs.edf_filename).replace(".edf", "_1d.dat")
        self.outputs.filename_out = filename_out
        self.outputs.index = index + 1

class ProxyOpenDataMask(
    Task,
    input_names=["edf_filename"],
    output_names=["image"],
):
    def run(self):
        self.outputs.image = fabio.open(self.inputs.edf_filename).data

class ProxyPrint(
    Task,
    input_names=["image", "geometry", "energy", "detector"],
    output_names=["image", "geometry", "energy", "detector"],
):
    def run(self):
        # print("I'm at ProxyPrint")
        image = self.inputs.image
        geometry = self.inputs.geometry
        energy = self.inputs.energy
        detector = self.inputs.detector
        print(image.shape)
        print(geometry)
        print(energy)
        print(detector)
        self.outputs.image = image
        self.outputs.geometry = geometry
        self.outputs.energy = energy
        self.outputs.detector = detector



def single_self_calling():
    node_open = {"id" : "node_open", "task_type" : "class", "task_identifier" : "persistence.ProxyOpenManyData"}
    node_open_mask = {"id" : "node_open_mask", "task_type" : "class", "task_identifier" : "persistence.ProxyOpenDataMask"}
    node_config = {"id" : "node_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_print = {"id" : "node_print", "task_type" : "class", "task_identifier" : "persistence.ProxyPrint"}
    node_integrate = {"id" : "node_integrate", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1D"}
    node_save = {"id" : "node_save", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.ascii.SaveAsciiPattern1D"}

    link_open_to_print = {
        "source" : "node_open",
        "target" : "node_print",
        "data_mapping" : [
            {"source_output" : "image", "target_input" : "image"},
        ]
    }

    link_open_self = {
        "source" : "node_open",
        "target" : "node_open",
        "data_mapping" : [
            {"source_output" : "list_filenames", "target_input" : "list_filenames"},
            {"source_output" : "index", "target_input" : "index"},
        ],
        "conditions" : [{"source_output": "repeat", "value": True}],
    }

    link_mask_to_integrate = {
        "source" : "node_open_mask",
        "target" : "node_integrate",
        "data_mapping" : [
            {"source_output" : "image", "target_input" : "mask"},
        ]
    }

    link_config_to_print = {
        "source" : "node_config",
        "target" : "node_print",
        "data_mapping" : [
            {"source_output" : "geometry", "target_input" : "geometry"},
            {"source_output" : "energy", "target_input" : "energy"},
            {"source_output" : "detector", "target_input" : "detector"},
        ]
    }

    link_print_to_integrate = {
        "source" : "node_print",
        "target" : "node_integrate",
        "data_mapping" : [
            {"source_output" : "geometry", "target_input" : "geometry"},
            {"source_output" : "energy", "target_input" : "energy"},
            {"source_output" : "detector", "target_input" : "detector"},
            {"source_output" : "image", "target_input" : "image"},
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

    link_filenameout = {
        "source" : "node_open",
        "target" : "node_save",
        "data_mapping" : [
            {"source_output" : "filename_out", "target_input" : "filename"},
        ]
    }


    graph = {
        "graph" : {"id" : "self_calling_1"},
        "nodes" : [
            node_open,
            node_open_mask,
            node_config, 
            node_print, 
            node_integrate,
            node_save,
        ],
        "links" : [
            link_open_to_print,
            link_open_self,
            link_mask_to_integrate,
            link_config_to_print,
            link_print_to_integrate,
            link_integrate_to_save,
            link_filenameout,
        ],
    }
    convert_graph(graph, "self_calling_1.json")
    chunk_files = [str(item) for item in Path("edf_data/").glob("*000*.edf")]
    poni = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    mask = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"

    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[

            {"name" : "edf_filename", "value" : mask, "id" : "node_open_mask"},
            {"name" : "filename", "value" : poni, "id" : "node_config"},
            {"name" : "list_filenames", "value" : chunk_files, "id" : "node_open"},            
        ],
    )