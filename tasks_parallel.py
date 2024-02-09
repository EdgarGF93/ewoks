from ewokscore.task import Task
import ewoksxrpd
import time
from ewokscore import TaskWithProgress
import matplotlib.pyplot as plt
from ewoks import execute_graph, save_graph, convert_graph
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



