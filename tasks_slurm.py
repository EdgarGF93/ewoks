
from ewokscore import Task
from ewoks import execute_graph, convert_graph
import os

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

def generate_workflow_dummy(execute=True):
    node_dummy = {"id" : "node_dummy", "task_type" : "class", "task_identifier" : "tasks_slurm.Write"}
    graph = {"graph" : {"id" : "dummy_graph"}, "nodes" : [node_dummy], "links" : []}
    convert_graph(graph, "workflows/dummy_workflow.json")
    if execute:
        execute_graph(graph)


class ExecuteDaskSLURM(
    Task,
    input_names=["chunked_list", "poni", "npt", "method"],
):
    def run(self):
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



def activate_slurm_environment():
    os.environ["SLURM_USER"] = "edgar1993a"
    os.environ["SLURM_URL"] = "http://slurm-api.esrf.fr:6820"
    os.environ["SLURM_TOKEN"] = "SLURM_JWT=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDgxNTg0MzksImlhdCI6MTcwODA3MjAzOSwic3VuIjoiZWRnYXIxOTkzYSJ9.-N-CYsyeybGifEiymJDudFDQDjdZPQFo9K6_qmnyXWU"

if __name__ == "__main__":
    activate_slurm_environment()
    
