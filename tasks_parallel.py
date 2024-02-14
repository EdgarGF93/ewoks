from ewokscore.task import Task
import ewoksxrpd
import time
from ewokscore import TaskWithProgress
import matplotlib.pyplot as plt
from ewoks import execute_graph, save_graph, convert_graph
from pathlib import Path
import h5py, hdf5plugin
import fabio
class SplitFrames(
        Task,
        input_names=["h5file"],
        optional_input_names=["index_frame"],
        output_names=["frame"],
    ):
    def run(self):
        pass

class PrintFrame(
        Task,
        input_names=["frame"],
    ):
    def run(self):
        print(self.inputs.frame)

class PrintString(
    TaskWithProgress,
    input_names=["string"],
):
    def run(self):        
        if isinstance(self.inputs.string, str):
            list_strings = self.inputs.string.split(" ")
        else:
            list_strings = self.inputs.string

        for s in list_strings:
            print(s)
            time.sleep(0.2)

class PlotImage(
    Task,
    input_names=["image"],
):
     def run(self):
         plt.imshow(self.inputs.image, vmin=0, vmax=500)
         plt.colorbar()
         plt.savefig("fig.png")
        #  plt.show()
         


class InputNombre(
    Task,
    input_names=["input_nombre"],
    output_names=["ok", "output_nombre"],
):
    def run(self):
        nombre = self.inputs.input_nombre
        if isinstance(nombre, str):
            self.outputs.output_nombre = nombre
            self.outputs.ok = True
        else:
            self.outputs.ok = False
            self.outputs.output_nombre = ""

class PrintHola(
    Task,
    input_names=["nombre"],
):
    def run(self):
        print(f"Hola {self.inputs.nombre}")

class PrintAdios(
    Task,
    input_names=["nombre"],
):
    def run(self):
        print(f"Adios {self.inputs.nombre}")


class Launcher(
    Task,
    input_names=["_run"],
    output_names=["go"],
):
    def run(self):
        if self.inputs._run == True:
            self.outputs.go = True
        else:
            self.outputs.go = False


class Wait1s(
    Task,
    input_names=["_run"],
):
    def run(self):
        if self.inputs._run == True:
            time.sleep(1)
        else:
            pass


class Benchmark(
    Task,
    input_names=["start", "stop"],
    output_names=["benchmark"],
):
    def run(self):
        if self.inputs.start:
            st = time.perf_counter()










def generate_branch_wait(node_source:dict, id_node_target:str):
    wait_workflow = {
    "graph" : {"id" : f"wait_{id_node_target}"},
    "nodes" : [node_source,
               {"id" : id_node_target, "task_type" : "class", "task_identifier" : "tasks_parallel.Wait1s"},
               ],
    "links" : [{"source" : node_source["id"], "target" : id_node_target, "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}]},
               ]
    }
    return wait_workflow




def generate_node_link_wait(node_source:dict, id_node_target:str):
    node_target = {
        "id" : id_node_target, 
        "task_type" : "class", 
        "task_identifier" : "tasks_parallel.Wait1s",
    }
    link = {
        "source" : node_source["id"], 
        "target" : id_node_target, 
        "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}],
    }
    return node_target, link




def generate_wait_branched_graph(nbranches=5):
    node_launcher = {
        "id" : "node_launcher", 
        "task_type" : "class", 
        "task_identifier" : "tasks_parallel.Launcher",
    }
    nodes = [node_launcher]
    links = []
    for ind in range(nbranches):
        node_target, link = generate_node_link_wait(node_source=node_launcher, id_node_target=f"node_wait_{ind}")
        nodes.append(node_target)
        links.append(link)
    id_graph = f"workflow_{nbranches}_branches"
    graph = {
        "graph" : {"id" : id_graph},
        "nodes" : nodes,
        "links" : links,
    }
    return graph

from ewoksxrpd.tasks import pyfaiconfig
import ewokscore
# class SplitH5(
#     Task,
#     input_names=["h5_file", "chunk_size", "chunk_index", "_continue"],
#     output_names=["dataset", "chunk_index", "_continue", "output_file"],
# ):
#     def run(self):
#         if self.missing_inputs._continue:
#             _continue = True
#         else:
#             _continue = self.inputs._continue

#         if not _continue:
#             self.outputs.dataset = None
#         else:
#             if self.missing_inputs.chunk_index:
#                 chunk_index = 0
#             else:
#                 chunk_index = self.inputs.chunk_index
            
#             if self.missing_inputs.chunk_size:
#                 chunk_size = 100
#             else:
#                 chunk_size = self.inputs.chunk_size

#             range_frame = [chunk_index * chunk_size, (chunk_index + 1) * chunk_size]
#             with h5py.File(self.inputs.h5_file) as f:
#                 full_dataset = f["entry_0000"]["measurement"]["data"]
#                 dataset = full_dataset[range_frame[0]:range_frame[1]]

#             if len(dataset) != chunk_size:
#                 self.outputs._continue = False
#             else:
#                 self.outputs._continue = True

#             self.outputs.dataset = dataset
#             self.outputs.output_file = f"integration_chunk_{chunk_index}.h5"
#             self.outputs.chunk_index = chunk_index + 1


            



        

def integrate_workflow(poni_file:str=""):
    node_poni = {"id" : "node_poni", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig"}
    node_h5 = {"id" : "node_h5", "task_type" : "class", "task_identifier" : "tasks_parallel.SplitH5"}
    node_int = {"id" : "node_int", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.Integrate1DList"}
    node_save = {}
    
    link_poni = {"source" : "node_poni", "target" : "node_int", "data_mapping" : [{"source_output" : "energy", "target_input" : "energy"},
                                                                                 {"source_output" : "detector", "target_input" : "detector"},
                                                                                 {"source_output" : "geometry", "target_input" : "geometry"},
                                                                                 ]}
    link_h5 = {"source" : "node_h5", "target" : "node_int", "data_mapping" : [{"source_output" : "dataset", "target_input" : "images"},
                                                                              {"source_output" : "output_file", "target_input" : "output_file"},]}

    link_self_h5 = {"source" : "node_h5", "target" : "node_h5", "data_mapping" : [{"source_output" : "_continue", "target_input" : "_continue"},
                                                                                  {"source_output" : "chunk_index", "target_input" : "chunk_index"},],
                                                                                  "conditions": [{"source_output": "result", "value": 10}]
                                                                                  }

    link_save = {"source" : "node_int", "target" : "node_save"}

    graph_h5 = {
        "graph" : {"id" : "h5_branch"},
        "nodes" : [
            node_poni, 
            node_h5, 
            node_int,
        ],
        "links" : [
            link_poni, 
            link_h5, 
            link_self_h5, 
            # link_save,
        ],
    }

    convert_graph(graph_h5, "graph_self_h5.json")





class SplitString(
    Task,
    input_names=["list_strings", "index"],
    output_names=["str_out", "index", "_continue"],
):
    def run(self):
        print('hoa')
        if isinstance(self.inputs.list_strings, str):
            list_strings = self.inputs.list_strings.split()
        elif isinstance(self.inputs.list_strings, list):
            list_strings = self.inputs.list_strings

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index
        self.outputs.str_out = list_strings[index]
        index += 1

        if index == len(list_strings):
            self.outputs._continue = False
        else:
            self.outputs._continue = True

class PrintString(
    Task,
    input_names=["msg"],
):
    def run(self):
        print(self.inputs.msg)



def test_conditional():
    node_self = {"id" : "node_self", "task_type" : "class", "task_identifier" : "tasks_parallel.SplitString"}
    node_print = {"id" : "node_print", "task_type" : "class", "task_identifier" : "tasks_parallel.PrintString"}
    link_print = {"source" : "node_self", "target" : "node_print", "data_mapping" : [{"source_output" : "str_out", "target_input" : "msg"}]}
    link_self = {"source" : "node_self", "target" : "node_self", "data_mapping" : [{"source_output" : "index", "target_input" : "index"},
                                                                                   ],
                 "conditions" : [{"source_output" : "_continue", "value" : True}]
                 }
    graph = {
        "graph" : {"id" : "graph_self"},
        "nodes" : [node_self, node_print],
        "links" : [link_self, link_print],
    }

    convert_graph(graph, "test_self.json")

    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[{"name" : "list_strings", "value" : "hola edgar"}],
    )


class LoopPrint(
    Task,
    optional_input_names = ["string", "time"],
    output_names = ["repeat", "time"],
):
    def run(self):
        string = self.get_input_value(key="string", default="hello")
        if self.missing_inputs.time:
            time = 1
        else:
            time = self.inputs.time
        print(string)
        self.outputs.time = time + 1

        if time == 5:
            self.outputs.repeat = False
        else:
            self.outputs.repeat = True

def test_conditional_2():
    node_print = {
        "id" : "node_print",
        "task_type" : 
        "class", "task_identifier" : "tasks_parallel.LoopPrint",
    }

    link_self = {
        "source" : "node_print", 
        "target" : "node_print", 
        "data_mapping" : [{"source_output" : "time", "target_input" : "time"}],
        "conditions": [{"source_output": "repeat", "value": True}],
    }
        
    graph = {
        "graph" : {"id" : "graph_loop"},
        "nodes" : [node_print],
        "links" : [link_self],
    }
    convert_graph(graph, "test_loop.json")
    execute_graph(
        graph=graph,
        engine="dask",
    )
    



class MeasureH5(
    Task,
    input_names=["h5file"],
    output_names=["Nframes"],
):
    def run(self):
        h5_file = self.inputs.h5file
        if Path(h5_file).is_file():
            with h5py.File(h5_file) as f:
                Nframes = f["entry_0000"]["measurement"]["data"][()].shape[0]
            self.outputs.Nframes = Nframes






class SplitH5(
    Task,
    input_names=["Nframes"],
    optional_input_names=["chunk_size", "Nframes", "chunk_index"],
    output_names=["chunk_size", "Nframes", "chunk_index", "frame_range", "repeat"],
):
    def run(self):
        Nframes = self.inputs.Nframes
        self.outputs.Nframes = Nframes

        if self.missing_inputs.chunk_size:
            chunk_size = 100
        else:
            chunk_size = self.inputs.chunk_size
        if self.missing_inputs.chunk_index:
            chunk_index = 0
        else:
            chunk_index = self.inputs.chunk_index
        frame_range = [chunk_index * chunk_size, (chunk_index + 1) * chunk_size]

        if frame_range[0] >= Nframes:
            self.outputs.frame_range = None
            self.outputs.repeat = False
        elif frame_range[1] >= Nframes:
            self.outputs.frame_range = frame_range
            self.outputs.repeat = False
        else:
            self.outputs.frame_range = frame_range
            self.outputs.repeat = True
        self.outputs.chunk_size = chunk_size
        self.outputs.chunk_index = chunk_index + 1




class PrintRange(
    Task,
    input_names=["frame_range"],
):
    def run(self):
        print(self.inputs.frame_range)











def split_h5():
    node_measure = {"id" : "node_measure", "task_type" : "class", "task_identifier" : "tasks_parallel.MeasureH5"}
    node_split = {"id" : "node_split", "task_type" : "class", "task_identifier" : "tasks_parallel.SplitH5"}
    node_print = {"id" : "node_print", "task_type" : "class", "task_identifier" : "tasks_parallel.PrintRange"}

    link_measure = {"source" : "node_measure", "target" : "node_split", "data_mapping" : [{"source_output" : "Nframes", "target_input" : "Nframes"}]}
    link_split = {"source" : "node_split", "target" : "node_print", "data_mapping" : [{"source_output" : "frame_range", "target_input" : "frame_range"}]}
    link_split_self = {"source" : "node_split", "target" : "node_split", "data_mapping" : [{"source_output" : "chunk_size", "target_input" : "chunk_size"},
                                                                                           {"source_output" : "Nframes", "target_input" : "Nframes"},
                                                                                           {"source_output" : "chunk_index", "target_input" : "chunk_index"},
                                                                                           ],
                                                                         "conditions" : [{"source_output": "repeat", "value": True}],
                    }

    graph = {
        "graph" : {"id" : "graph_splith5"},
        "nodes" : [node_measure, node_split, node_print],
        "links" : [link_measure, link_split, link_split_self],
    }
    convert_graph(graph, "test_h5_split.json")
    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[
            {"name" : "h5file", "value" : "/users/edgar1993a/work/ewoks_parallel/p1m_dummy_100frames.h5", "id" : "node_measure"},
            {"name" : "chunk_size", "value" : 17, "id" : "node_split"},
        ]
    )

class SliceH5(
    Task,
    input_names=["h5file", "frame_range"],
    output_names=["dataset", "output_file"],
):
    def run(self):
        h5_file = self.inputs.h5file
        frame_range = self.inputs.frame_range
        if Path(h5_file).is_file():
            with h5py.File(h5_file) as f:
                dataset = f["entry_0000"]["measurement"]["data"][frame_range[0]:frame_range[1]]
            self.outputs.dataset = dataset
            self.outputs.output_file = h5_file.replace(".h5", f"_{str(frame_range)}.h5")




def integrate_h5_section():
    node_measure = {"id" : "node_measure", "task_type" : "class", "task_identifier" : "tasks_parallel.MeasureH5"}
    node_split = {"id" : "node_split", "task_type" : "class", "task_identifier" : "tasks_parallel.SplitH5"}
    node_slice_h5 = {"id" : "node_slice_h5", "task_type" : "class", "task_identifier" : "tasks_parallel.SliceH5"}
    node_pyfai_config = {"id" : "node_pyfai_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_integrate = {"id" : "node_integration", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1DList"}

    link_measure = {"source" : "node_measure", "target" : "node_split", "data_mapping" : [{"source_output" : "Nframes", "target_input" : "Nframes"}]}
    link_split = {"source" : "node_split", "target" : "node_slice_h5", "data_mapping" : [{"source_output" : "frame_range", "target_input" : "frame_range"}]}
    link_split_self = {"source" : "node_split", "target" : "node_split", "data_mapping" : [{"source_output" : "chunk_size", "target_input" : "chunk_size"},
                                                                                           {"source_output" : "Nframes", "target_input" : "Nframes"},
                                                                                           {"source_output" : "chunk_index", "target_input" : "chunk_index"},
                                                                                           ],
                                                                         "conditions" : [{"source_output": "repeat", "value": True}],
                    }
    link_config = {"source" : "node_pyfai_config", "target" : "node_integration", "data_mapping" : [{"source_output" : "energy", "target_input" : "energy"},
                                                                                              {"source_output" : "detector", "target_input" : "detector"},
                                                                                              {"source_output" : "geometry", "target_input" : "geometry"},
                                                                                              ]}
    link_integration = {"source" : "node_slice_h5", "target" : "node_integration", "data_mapping" : [{"source_output" : "dataset", "target_input" : "images"},
                                                                                                     {"source_output" : "output_file", "target_input" : "output_file"},
                                                                                                     ]}
    
    

    FILE_H5 = "/users/edgar1993a/work/ewoks_parallel/p1m_dummy_1000frames.h5"
    OUTPUT_FILE = "/users/edgar1993a/work/ewoks_parallel/p1m_dummy_10frames_out.h5"
    PONI_FILE = "/users/edgar1993a/work/ewoks_parallel/fake_poni.poni"
    CHUNK = 10
    INTEGRATION_OPTIONS = {
        "npt_rad" : 2000,
    }

    graph = {
        "graph" : {"id" : "integrate_slice_h5"},
        "nodes" : [node_measure, node_split, node_slice_h5, node_pyfai_config, node_integrate],
        "links" : [link_measure, link_split, link_split_self, link_config, link_integration],
    }

    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[
            {"name" : "h5file", "value" : FILE_H5, "id" : "node_measure"},
            {"name" : "h5file", "value" : FILE_H5, "id" : "node_slice_h5"},
            {"name" : "chunk_size", "value" : CHUNK, "id" : "node_split"},
            {"name" : "filename", "value" : PONI_FILE, "id" : "node_pyfai_config"},
            # {"name" : "output_file", "value" : OUTPUT_FILE, "id" : "node_integration"},
            # {"name" : "integration_options", "value" : INTEGRATION_OPTIONS, "id" : "node_integration"},
        ]
    )

def direct_integration():
    node_pyfai_config = {"id" : "node_pyfai_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_integrate = {"id" : "node_integration", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1DList"}
    link_config = {"source" : "node_pyfai_config", "target" : "node_integration", "data_mapping" : [{"source_output" : "energy", "target_input" : "energy"},
                                                                                                    {"source_output" : "detector", "target_input" : "detector"},
                                                                                                    {"source_output" : "geometry", "target_input" : "geometry"},
                                                                                  ]}
    
    # FILE_H5 = "/users/edgar1993a/work/ewoks_parallel/p1m_dummy_1000frames.h5"
    # PONI_FILE = "/users/edgar1993a/work/ewoks_parallel/fake_poni.poni"
    FILE_H5 = "/users/edgar1993a/work/UM_2024/Eiger2_CeO2_75keV.h5"
    OUTPUT_FILE = "/users/edgar1993a/work/ewoks_parallel/Eiger2_CeO2_75keV_out.h5"
    PONI_FILE = "/users/edgar1993a/work/UM_2024/eiger.poni"


    graph = {
        "graph" : {"id" : "integrate_slice_h5"},
        "nodes" : [node_pyfai_config, node_integrate],
        "links" : [link_config],
    }

    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[
            {"name" : "images", "value" : FILE_H5, "id" : "node_integration"},
            {"name" : "output_file", "value" : OUTPUT_FILE, "id" : "node_integration"},
            {"name" : "filename", "value" : PONI_FILE, "id" : "node_pyfai_config"},
        ]
    )



def direct_integration_edf_files():
    node_pyfai_config = {"id" : "node_pyfai_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_integrate = {"id" : "node_integration", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1DList"}
    link_config = {"source" : "node_pyfai_config", "target" : "node_integration", "data_mapping" : [{"source_output" : "energy", "target_input" : "energy"},
                                                                                                    {"source_output" : "detector", "target_input" : "detector"},
                                                                                                    {"source_output" : "geometry", "target_input" : "geometry"},
                                                                                  ]}
    
    FILES = [str(item) for item in Path("/users/edgar1993a/work/ewoks_parallel/edf_data").glob("*.edf")]
    OUTPUT_FILE = "/users/edgar1993a/work/ewoks_parallel/edf_dummy_out.h5"
    PONI_FILE = "/users/edgar1993a/work/ewoks_parallel/fake_poni.poni"
    graph = {
        "graph" : {"id" : "integrate_edf_1"},
        "nodes" : [
            node_pyfai_config, 
            node_integrate, 
        ],
        "links" : [
            link_config, 
        ],
    }
    convert_graph(graph, "edf_integration.json")
    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[
            {"name" : "images", "value" : FILES, "id" : "node_integration"},
            {"name" : "output_file", "value" : OUTPUT_FILE, "id" : "node_integration"},
            {"name" : "filename", "value" : PONI_FILE, "id" : "node_pyfai_config"},
        ]
    )



class SplitList(
    Task,
    input_names=["list_filenames"],
    optional_input_names=["chunk_size", "chunk_index"],
    output_names=["chunk_size", "list_filenames", "chunk_index", "images", "repeat", "output_file"],
):
    def run(self):
        list_filenames = self.inputs.list_filenames
        self.outputs.list_filenames = list_filenames

        if self.missing_inputs.chunk_size:
            chunk_size = 100
        else:
            chunk_size = self.inputs.chunk_size
        if self.missing_inputs.chunk_index:
            chunk_index = 0
        else:
            chunk_index = self.inputs.chunk_index
        frame_range = [chunk_index * chunk_size, (chunk_index + 1) * chunk_size]

        self.outputs.chunk_size = chunk_size
        self.outputs.chunk_index = chunk_index + 1
        self.outputs.output_file = f"/users/edgar1993a/work/ewoks_parallel/edf_dummy_out_{chunk_index}.h5"



        if frame_range[0] >= len(list_filenames):
            self.outputs.images = None
            self.outputs.repeat = False
        elif frame_range[1] >= len(list_filenames):
            self.outputs.images = list_filenames[frame_range[0]:frame_range[1]]
            self.outputs.repeat = False
        else:
            self.outputs.images = list_filenames[frame_range[0]:frame_range[1]]
            self.outputs.repeat = True

        self.outputs


class Buffer(
    Task,
    optional_input_names=["detector", "energy", "geometry", "images", "output_file"],
    output_names = ["detector", "energy", "geometry", "images", "output_file", "_continue"],
):
    def run(self):
        self.outputs.detector = self.get_input_value(key="detector", default=None)
        self.outputs.energy = self.get_input_value(key="energy", default=None)
        self.outputs.geometry = self.get_input_value(key="geometry", default=None)
        self.outputs.images = self.get_input_value(key="images", default=None)
        self.outputs.output_file = self.get_input_value(key="output_file", default=None)
        if self.outputs.detector and self.outputs.energy and self.outputs.geometry and self.outputs.images and self.outputs.output_file:
            self.outputs._continue = True
        else:
            self.outputs._continue = False

class DummyIntegrationList(
    Task,
    input_names=["detector", "energy", "geometry", "images", "output_file"],
):
    def run(self):
        print(self.inputs.images)
        print(self.inputs.output_file)





def parallel_integration_edf_files():
    node_pyfai_config = {"id" : "node_pyfai_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_split = {"id" : "node_split", "task_type" : "class", "task_identifier" : "tasks_parallel.SplitList"}
    node_buffer = {"id" : "node_buffer", "task_type" : "class", "task_identifier" : "tasks_parallel.Buffer"}    
    node_integrate = {"id" : "node_integrate", "task_type" : "class", "task_identifier" : "tasks_parallel.Integrate1DList"}

    link_split = {
        "source" : "node_split", 
        "target" : "node_buffer", 
        "data_mapping" : [
            {"source_output" : "images", "target_input" : "images"},
            {"source_output" : "output_file", "target_input" : "output_file"},
        ],
    }

    link_self = {
        "source" : "node_split", 
        "target" : "node_split", 
        "data_mapping" : [{"source_output" : "chunk_size", "target_input" : "chunk_size"},
                          {"source_output" : "list_filenames", "target_input" : "list_filenames"},
                          {"source_output" : "chunk_index", "target_input" : "chunk_index"},
                          ],
        "conditions" : [{"source_output": "repeat", "value": True}],
    }

    link_config = {
        "source" : "node_pyfai_config", 
        "target" : "node_buffer", 
        "data_mapping" : [
            {"source_output" : "energy", "target_input" : "energy"},
            {"source_output" : "detector", "target_input" : "detector"},
            {"source_output" : "geometry", "target_input" : "geometry"},
        ],
    }
    
    link_integration = {
        "source" : "node_buffer", 
        "target" : "node_integrate", 
        "data_mapping" : [
            {"source_output" : "images", "target_input" : "images"},
            {"source_output" : "output_file", "target_input" : "output_file"},
            {"source_output" : "energy", "target_input" : "energy"},
            {"source_output" : "detector", "target_input" : "detector"},
            {"source_output" : "geometry", "target_input" : "geometry"},
        ],
        "conditions" : [{"source_output": "_continue", "value": True}],
    }

    FILES = [str(item) for item in Path("/users/edgar1993a/work/ewoks_parallel/edf_data").glob("*.edf")]
    OUTPUT_FILE = "/users/edgar1993a/work/ewoks_parallel/edf_dummy_out.h5"
    PONI_FILE = "/users/edgar1993a/work/ewoks_parallel/fake_poni.poni"
    graph = {
        "graph" : {"id" : "integrate_edf_1"},
        "nodes" : [node_split, node_pyfai_config, node_integrate, node_buffer],
        "links" : [link_self, link_split, link_config, link_integration],
    }
    convert_graph(graph, "edf_integration.json")
    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[
            {"name" : "list_filenames", "value" : FILES, "id" : "node_split"},
            {"name" : "filename", "value" : PONI_FILE, "id" : "node_pyfai_config"},
        ],
    )


class Proxy(
    Task,
    input_names=["images"],
    output_names=["images", "output_file"],
):
    def run(self):
        self.outputs.images = self.inputs.images
        self.outputs.output_file = self.inputs.output_file



def integrate_h5():
    node_pyfai_config = {"id" : "node_pyfai_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}
    node_proxy = {"id" : "node_proxy", "task_type" : "class", "task_identifier" : "tasks_parallel.Proxy"}
    node_integrate = {"id" : "node_integrate", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1DList"}

    link_proxy = {
        "source" : "node_proxy", 
        "target" : "node_integrate", 
        "data_mapping" : [
            {"source_output" : "images", "target_input" : "images"},
            {"source_output" : "output_file", "target_input" : "output_file"},
        ],
    }

    link_config = {
        "source" : "node_pyfai_config", 
        "target" : "node_integrate", 
        "data_mapping" : [
            {"source_output" : "energy", "target_input" : "energy"},
            {"source_output" : "detector", "target_input" : "detector"},
            {"source_output" : "geometry", "target_input" : "geometry"},
        ],
    }

    INPUT_H5 = "/users/edgar1993a/work/ewoks_parallel/Eiger2_CeO2_75keV.h5"
    OUTPUT_H5 = "/users/edgar1993a/work/ewoks_parallel/Eiger2_CeO2_75keV_out.h5"
    PONI_FILE = "/users/edgar1993a/work/ewoks_parallel/eiger.poni"

    graph = {
        "graph" : {"id" : "integrate_h5"},
        "nodes" : [node_proxy, node_pyfai_config, node_integrate],
        "links" : [link_proxy, link_config],
    }

    convert_graph(graph, "integrate_h5.json")
    execute_graph(
        graph=graph,
        engine="dask",
        inputs=[
            {"name" : "images", "value" : INPUT_H5, "id" : "node_proxy"},
            {"name" : "output_file", "value" : OUTPUT_H5, "id" : "node_proxy"},
            {"name" : "filename", "value" : PONI_FILE, "id" : "node_pyfai_config"},
        ],
    )







if __name__ == "__main__":
    st = time.perf_counter()
    integrate_h5()
    print(f"Benchmark: {time.perf_counter() - st:.2f} s.")


