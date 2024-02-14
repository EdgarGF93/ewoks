from ewokscore import Task
from ewoks import convert_graph, execute_graph
import time
import numpy as np
import matplotlib.pyplot as plt
import fabio
from pathlib import Path

class Proxy1(
    Task,
    input_names=["attr"],
    output_names=["out"],
):
    def run(self):
        with open("kk/countproxy1.txt", 'a+') as f:
            f.write(f"{time.perf_counter()}\n")
        self.outputs.out = self.inputs.attr

class Proxy2(
    Task,
    input_names=["attr"],
    output_names=["out"],
):
    def run(self):
        with open("kk/countproxy2.txt", 'a+') as f:
            f.write(f"{time.perf_counter()}\n")
        self.outputs.out = self.inputs.attr

class Print(
    Task,
    input_names=["string1", "string2"],
):
    def run(self):
        pass
        # print(f"{self.inputs.string1}")
        # print(f"{self.inputs.string2}")

def persistence():
    node1 = {"id" : "node_1", "task_type" : "class", "task_identifier" : "persistence.Proxy1"}
    node2 = {"id" : "node_2", "task_type" : "class", "task_identifier" : "persistence.Proxy2"}
    node3 = {"id" : "node_3", "task_type" : "class", "task_identifier" : "persistence.Print"}

    link_13 = {
        "source" : "node_1",
        "target" : "node_3",
        "data_mapping" : [{"source_output" : "out", "target_input" : "string1"},
        ]
    }
    link_23 = {
        "source" : "node_2",
        "target" : "node_3",
        "data_mapping" : [{"source_output" : "out", "target_input" : "string2"},
        ]
    }

    graph = {
        "graph" : {"id" : "graph_persistence"},
        "nodes" : [node1, node2, node3],
        "links" : [link_23, link_13],
    }

    convert_graph(graph, "graph_persistence.json")
    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[

            {"name" : "attr", "value" : "hello from node 1", "id" : "node_1"},
            {"name" : "attr", "value" : "hello from node 2", "id" : "node_2"},
        ],
    )



def get_wins():
    with open(f"kk/countproxy1.txt", "r") as f:
        counts_1 = [float(line.strip()) for line in f.readlines()]
    with open(f"kk/countproxy2.txt", "r") as f:
        counts_2 = [float(line.strip()) for line in f.readlines()]
    win1 = 0
    win2 = 0
    for c1, c2 in zip(counts_1, counts_2):
        if c1 < c2:
            win1 += 1
        else:
            win2 += 1
    x = np.array(["Node1", "Node2"])
    y = np.array([win1, win2])
    plt.bar(x=x, height=y)
    plt.ylabel("Times")
    plt.savefig("Wins_ppf_1000.png")





class ProxyOpenData(
    Task,
    input_names=["edf_filename"],
    output_names=["image", "filename_out"],
):
    def run(self):
        self.outputs.image = fabio.open(self.inputs.edf_filename).data
        filename_out = str(self.inputs.edf_filename).replace(".edf", "_1d.dat")
        self.outputs.filename_out = filename_out

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
        # print(image.shape)
        # print(geometry)
        # print(energy)
        # print(detector)
        self.outputs.image = image
        self.outputs.geometry = geometry
        self.outputs.energy = energy
        self.outputs.detector = detector

def persistence_config():
    node_open = {"id" : "node_open", "task_type" : "class", "task_identifier" : "persistence.ProxyOpenData"}
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
        "graph" : {"id" : "persistence_config"},
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
            link_mask_to_integrate,
            link_config_to_print,
            link_print_to_integrate,
            link_integrate_to_save,
            link_filenameout,
        ],
    }
    convert_graph(graph, "persistence_config_2.json")

    # EDF_FILENAME = "/users/edgar1993a/work/ewoks_parallel/LaB6.edf"
    # PONI_EDF = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    # MASK = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"

    # execute_graph(
    #     graph=graph,
    #     engine="ppf",
    #     inputs=[
    #         {"name" : "edf_filename", "value" : EDF_FILENAME, "id" : "node_open"},
    #         {"name" : "edf_filename", "value" : MASK, "id" : "node_open"},
    #         {"name" : "filename", "value" : PONI_EDF, "id" : "node_config"},
    #     ],
    # )

def generate_integration_wf(filename_list):

    node_open_mask = {"id" : "node_open_mask", "task_type" : "class", "task_identifier" : "persistence.ProxyOpenDataMask"}
    node_config = {"id" : "node_config", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.pyfaiconfig.PyFaiConfig"}

    nodes = [node_open_mask, node_config]
    links = []
    for index, filename in enumerate(filename_list):
        tag = str(filename)
        nodes.append({
            "id" : f"node_open_{tag}", 
            "task_type" : "class", 
            "task_identifier" : "persistence.ProxyOpenData", 
            "default_inputs":[{"name" : "edf_filename", "value" : str(filename)}]})
        nodes.append({"id" : f"node_print_{tag}", "task_type" : "class", "task_identifier" : "persistence.ProxyPrint"})
        nodes.append({"id" : f"node_integrate_{tag}", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.integrate.Integrate1D"})
        nodes.append({"id" : f"node_save_{tag}", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.ascii.SaveAsciiPattern1D"})

        links.append({
            "source" : f"node_open_{tag}",
            "target" : f"node_print_{tag}",
            "data_mapping" : [
                {"source_output" : "image", "target_input" : "image"},
            ]
        })

        links.append({
            "source" : "node_open_mask",
            "target" : f"node_integrate_{tag}",
            "data_mapping" : [
                {"source_output" : "image", "target_input" : "mask"},
            ]
        })

        links.append({
            "source" : "node_config",
            "target" : f"node_print_{tag}",
            "data_mapping" : [
                {"source_output" : "geometry", "target_input" : "geometry"},
                {"source_output" : "energy", "target_input" : "energy"},
                {"source_output" : "detector", "target_input" : "detector"},
            ]
        })

        links.append({
            "source" : f"node_print_{tag}",
            "target" : f"node_integrate_{tag}",
            "data_mapping" : [
                {"source_output" : "geometry", "target_input" : "geometry"},
                {"source_output" : "energy", "target_input" : "energy"},
                {"source_output" : "detector", "target_input" : "detector"},
                {"source_output" : "image", "target_input" : "image"},
            ]
        })

        links.append({
            "source" : f"node_integrate_{tag}",
            "target" : f"node_save_{tag}",
            "data_mapping" : [
                {"source_output" : "x", "target_input" : "x"},
                {"source_output" : "y", "target_input" : "y"},
                {"source_output" : "xunits", "target_input" : "xunits"},
            ]
        })

        links.append({
            "source" : f"node_open_{tag}",
            "target" : f"node_save_{tag}",
            "data_mapping" : [
                {"source_output" : "filename_out", "target_input" : "filename"},
            ]
        })


    graph = {
        "graph" : {"id" : "persistence_config"},
        "nodes" : nodes,
        "links" : links,
    }
    return graph
    # convert_graph(graph, "generation_integrators_edf.json")
    # execute_graph(
    #     graph=graph,
    #     engine="ppf",
    #     inputs=[
    #         {"name" : "edf_filename", "value" : MASK, "id" : "node_open_mask"},
    #         {"name" : "filename", "value" : PONI_EDF, "id" : "node_config"},
    #     ],
    # )




def generate_chunks_edf_integration(filename_list, mask_file, poni_file, chunk_size=5):
    for index in range(int(len(filename_list)/chunk_size) + 1):
        chunk_range = [index * chunk_size, (index + 1) * chunk_size]
        chunk = filename_list[chunk_range[0]:chunk_range[1]]
        graph = generate_integration_wf(filename_list=chunk)
        execute_graph(
            graph=graph,
            engine="ppf",
            inputs=[
                {"name" : "edf_filename", "value" : mask_file, "id" : "node_open_mask"},
                {"name" : "filename", "value" : poni_file, "id" : "node_config"},
            ],
        )






if __name__ == "__main__":



    # PONI_EDF = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    # MASK = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"
    # file = [str(item.absolute()) for item in Path("edf_data").glob("*.edf")][0]
    # st = time.perf_counter()
    # execute_graph(
    #     graph="persistence_config_2.json",
    #     engine="dask",
    #     inputs=[
    #         {"name" : "edf_filename", "value" : file, "id" : "node_open"},
    #         {"name" : "edf_filename", "value" : MASK, "id" : "node_open_mask"},
    #         {"name" : "filename", "value" : PONI_EDF, "id" : "node_config"},
    #     ],
    # )
    # print(f"Benchmark: {time.perf_counter() - st:.2f} s")




    # PONI_EDF = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    # MASK = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"
    # st = time.perf_counter()
    # list_files = [str(item.absolute()) for item in Path("edf_data").glob("*.edf")]
    # for file in list_files:
    #     execute_graph(
    #         graph="persistence_config_2.json",
    #         engine="ppf",
    #         inputs=[
    #             {"name" : "edf_filename", "value" : file, "id" : "node_open"},
    #             {"name" : "edf_filename", "value" : MASK, "id" : "node_open_mask"},
    #             {"name" : "filename", "value" : PONI_EDF, "id" : "node_config"},
    #         ],
    #     )
    # print(f"Benchmark PPF: {time.perf_counter() - st:.2f} s")





    # PONI_EDF = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    # MASK = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"
    # st = time.perf_counter()
    # list_files = [str(item.absolute()) for item in Path("edf_data").glob("*.edf")]
    # for file in list_files:
    #     execute_graph(
    #         graph="persistence_config_2.json",
    #         engine="dask",
    #         inputs=[
    #             {"name" : "edf_filename", "value" : file, "id" : "node_open"},
    #             {"name" : "edf_filename", "value" : MASK, "id" : "node_open_mask"},
    #             {"name" : "filename", "value" : PONI_EDF, "id" : "node_config"},
    #         ],
    #     )
    # print(f"Benchmark DASK: {time.perf_counter() - st:.2f} s")



    # FILENAME_LIST = [str(item.absolute()) for item in Path("edf_data").glob("*.edf")]
    # PONI_EDF = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    # MASK = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"

    # st = time.perf_counter()
    # generate_integration_wf(
    #     filename_list=FILENAME_LIST,
    #     mask_file=MASK,
    #     poni_file=PONI_EDF,    
    # )
    # print(f"Benchmark PPF: {time.perf_counter() - st:.2f} s")



    PONI_EDF = "/users/edgar1993a/work/ewoks_parallel/lab6.poni"
    MASK = "/users/edgar1993a/work/ewoks_parallel/lab6_mask.edf"
    FILENAME_LIST = sorted(Path("edf_data").glob("*.edf"))

    st = time.perf_counter()
    generate_chunks_edf_integration(filename_list=FILENAME_LIST, mask_file=MASK, poni_file=PONI_EDF)
    print(f"Benchmark PPF: {time.perf_counter() - st:.2f} s")
