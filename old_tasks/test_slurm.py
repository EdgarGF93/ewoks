from ewokscore import Task
import os
from ewoksjob.client import submit
from ewoks import convert_graph


class SlurmPrint(Task):
    def run(self):
        with open("test_slurm.txt", "a+") as f:
            f.write("hola\n")

def get_test_graph():
    nodes = [{"id" : "node", "task_type": "class", "task_identifier" : "test_slurm.SlurmPrint"}]
    graph = {"graph" : {"id" : "graph"}, "nodes" : nodes, "links" : []}    
    return graph

def run_test():
    graph = get_test_graph()
    convert_graph(graph, "test_slurm.json")

    # cmd = "ewoks submit test_slurm.json"
    # os.system(cmd)

    future = submit(args=(graph,))
    result = future.get(timeout=None)


if __name__ == "__main__":
    os.system("source activate_slurm_env.sh")
    run_test()