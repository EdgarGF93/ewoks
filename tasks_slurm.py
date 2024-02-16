
from ewokscore import Task
from ewoks import execute_graph, convert_graph
import os

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
