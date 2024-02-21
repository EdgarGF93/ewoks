from ewokscore import Task
from pyFAI import load
from ewoks import execute_graph
import fabio

class Proxy(
    Task,
    input_names=["cosa"],
    output_names=["cosa"],
):
    def run (self):
        self.outputs.cosa = self.inputs.cosa

class Print(
    Task,
    input_names=["cosa"],
):
    def run (self):
        pass

node1 = {"id" : f"node1", "task_type" : "class", "task_identifier" : "test_serialization.Proxy"}
node2 = {"id" : f"node2", "task_type" : "class", "task_identifier" : "test_serialization.Print"}
link = {
    "source" : "node1",
    "target" : "node2",
    "data_mapping" : [{"source_output" : "cosa", "target_input" : "cosa"},
    ]
}

graph = {
    "graph" : {"id" : f"graph"},
    "nodes" : [node1, node2],
    "links" : [link],
}


if __name__ == "__main__":
    ai = load("lab6.poni")
    sparse = ai.setup_sparse_integrator(shape=ai.detector.shape, npt=2000)
    execute_graph(
        graph=graph,
        engine="ppf",
        inputs=[{"name" : "cosa", "value" : sparse, "id" : "node1"}],
    )