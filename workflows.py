from ewoksxrpd.tasks.background import SubtractBackground
from tasks_parallel import PlotImage
import time

node_split_frames = {
    "id" : "node_split_frames",
    "task_type" : "class",
    "task_identifier" : "tasks_parallel.SplitFrames",
}

node_print = {
    "id" : "node_print",
    "task_type" : "class",
    "task_identifier" : "tasks_parallel.PrintFrame",
}

link_1 = {
    "source" : "node_split_frames",
    "target" : ""
}


node_print_string = {
    "id" : "node_print_string",
    "task_type" : "class",
    "task_identifier" : "tasks_parallel.PrintString",
    "default_inputs" : [{"name" : "string", "value" : "hola"}],
}

graph_print = {
    "graph" : {"id" : "wfprint"},
    "nodes" : [node_print_string],
}


graph_bg = {
    "graph" : {"id" : "bg"},
    "nodes" : [
        {"id" : "node1", "task_type" : "class", "task_identifier" : "ewoksxrpd.tasks.background.SubtractBackground"},
        {"id" : "node2", "task_type" : "class", "task_identifier" : "tasks_parallel.PlotImage"},
        ],
    "links" : [
        {"source" : "node1", "target" : "node2", "data_mapping" : [{"source_output" : "image", "target_input":"image"}]},
    ]
}



graph_bifurcation = {
    "graph" : {"id": "bif"},
    "nodes" : [{"id" : "node1", "task_type" : "class", "task_identifier" : "tasks_parallel.InputNombre"},
               {"id" : "node2", "task_type" : "class", "task_identifier" : "tasks_parallel.PrintHola"},
               {"id" : "node3", "task_type" : "class", "task_identifier" : "tasks_parallel.PrintAdios"},
                ],
    "links" : [{"source" : "node1", "target" : "node2", "data_mapping" : [{"source_output" : "output_nombre", "target_input" : "nombre"}]},
               {"source" : "node1", "target" : "node3", "data_mapping" : [{"source_output" : "output_nombre", "target_input" : "nombre"}]},
               ]
}


grahp_wait_parallel = {
    "graph" : {"id" : "wait"},
    "nodes" : [{"id" : "node1", "task_type" : "class", "task_identifier" : "tasks_parallel.Launcher"},
               {"id" : "node2", "task_type" : "class", "task_identifier" : "tasks_parallel.Wait1s"},
               {"id" : "node3", "task_type" : "class", "task_identifier" : "tasks_parallel.Wait1s"},
               {"id" : "node4", "task_type" : "class", "task_identifier" : "tasks_parallel.Wait1s"},
               {"id" : "node5", "task_type" : "class", "task_identifier" : "tasks_parallel.Wait1s"},
               {"id" : "node6", "task_type" : "class", "task_identifier" : "tasks_parallel.Wait1s"},
               ],
    "links" : [{"source" : "node1", "target" : "node2", "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}]},
               {"source" : "node1", "target" : "node3", "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}]},
               {"source" : "node1", "target" : "node4", "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}]},
               {"source" : "node1", "target" : "node5", "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}]},
               {"source" : "node1", "target" : "node6", "data_mapping" : [{"source_output" : "go", "target_input" : "_run"}]},
               ]
}





if __name__ == "__main__":
    from ewoks import execute_graph, save_graph, convert_graph
    strings = "hola buenas tardes me llamo edgar"


    # execute_graph(
    #     graph=graph_print,
    #     inputs = [{"name" : "string", "value" : strings}],
    # )


    # DATA_PATH = "/users/edgar1993a/work/ewoks_parallel/rayonix_A1_spitch_0.12_000_0000.edf"
    # BG_PATH = "/users/edgar1993a/work/ewoks_parallel/rayonix_10s_spitch_0.12_000_0000.edf"
    # execute_graph(
    #     graph=graph_bg,
    #     inputs=[
    #         {"name" : "image", "value" : DATA_PATH},
    #         {"name" : "monitor", "value" : 0.0},
    #         {"name" : "background", "value" : BG_PATH},
    #         {"name" : "background_monitor", "value" : 1.0},
    #     ]
    # )
    # convert_graph(graph_bg, "workflow_bg.json")


    # convert_graph(graph_bifurcation, "workflow_bif.json")
    # execute_graph(
    #     graph=graph_bifurcation,
    #     engine="ppf",
    #     inputs=[{"name":"input_nombre", "value" : "edgar"}],
    # )
    
    # convert_graph(grahp_wait_parallel, "workflow_wait.json")

    # st = time.perf_counter()
    # execute_graph(
    #     graph=grahp_wait_parallel,
    #     engine="dask",
    #     inputs=[{"name":"_run", "value" : True}],
    # )
    # print(f"Benchmark: {time.perf_counter() - st:.2f} s")


    from tasks_parallel import generate_wait_branched_graph
    import numpy as np
    import matplotlib.pyplot as plt
    x = np.logspace(0,3,100)
    y = []
    for nb in x:
        graph = generate_wait_branched_graph(nbranches=int(nb))
        # convert_graph(graph, f"{graph["graph"]["id"]}.json")
        st = time.perf_counter()
        execute_graph(
            graph=graph,
            engine="ppf",
            inputs=[{"name" : "_run", "value" : True}],
        )
        b = time.perf_counter() - st
        y.append(b)
    plt.plot(x,y, marker='o', ls='--')
    plt.xlabel("Branches")
    plt.ylabel("Time (s)")
    plt.savefig("benchmark.png")
    

