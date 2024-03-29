
from ewokscore import Task
from ewoks import execute_graph, convert_graph
import os
from pyFAI import load
from pathlib import Path
from itertools import islice
import json
from ewoksjob.client import submit
from pyFAI.io.image import read_data
import pyslurmutils

def generate_workflow_dummy(execute=True):
    node_dummy = {"id" : "node_dummy", "task_type" : "class", "task_identifier" : "tasks_config.Write"}
    graph = {"graph" : {"id" : "dummy_graph"}, "nodes" : [node_dummy], "links" : []}
    return graph
    # convert_graph(graph, "dummy_workflow.json")
    # if execute:
    #     execute_graph(graph)


def activate_slurm_env():
    os.system("source activate_slurm_env.sh")

def activate_redis():
    # type sudo service redis-server stop if its not opening
    os.system("source activate_redis.sh")

def activate_worker():
    os.system("source activate_worker.sh")

def submit_dummy_workflow():
    os.system("source submit_dummy.sh")


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

class OpenIntegrateSave(Task, input_names=["path_to_find", "chunk_range", "pattern", "config"]):
     def run(self):
        path = Path(self.inputs.path_to_find)
        pattern = self.inputs.pattern
        chunk_range = [int(_) for _ in self.inputs.chunk_range]
        list_filenames = [str(item) for item in islice(path.glob(pattern), chunk_range[0], chunk_range[1])]

        with open(self.inputs.config) as fp:
            config = json.load(fp)

        # WE CANNOT USE THE WORKER FOR OPENCL IMPLEMENTATION !
        # POSIBLY ISSUE WITH SPAWN/FORK MULTIPROCESSING
        # process(
        #     input_data=list_filenames,
        #     output=None,
        #     config=config,
        #     observer=None,
        #     monitor_name=None,
        # )

        ai = load(config)

        if config["do_mask"]:
            mask = read_data(config["mask_file"])
        else:
            mask = None

        if config["do_dark"]:
            dark = read_data(config["dark_current"])
        else:
            dark = None      

        if config["do_flat"]:
            flat = read_data(config["flat_field"])
        else:
            flat = None

        if config["do_radial_range"]:
            radial_range = [config["radial_range_min"], config["radial_range_max"]]
        else:
            radial_range = None

        if config["do_azimuthal_range"]:
            azimuth_range = [config["azimuth_range_min"], config["azimuth_range_max"]]
        else:
            azimuth_range = None

        if config["do_polarization"]:
            polarization_factor = config["polarization_factor"]
        else:
            polarization_factor = None

        for filename in list_filenames:
            res = ai.integrate1d(
                data=read_data(filename),
                npt=config["nbpt_rad"],
                filename=filename.replace(".edf", ".dat"),
                correctSolidAngle=config["do_solid_angle"],
                polarization_factor=polarization_factor,
                unit=config["unit"],
                error_model=config["error_model"],
                method=tuple(config["method"]),
                mask=mask,
                flat=flat,
                dark=dark,
                radial_range=radial_range,
                azimuth_range=azimuth_range,
            )
            # from pyFAI.method_registry import IntegrationMethod
            # with open("kkk.txt", "w") as f:
            #     f.write(str(IntegrationMethod.select_method(dim=1, split="bbox", algo="csr", impl="opencl")))
class SplitList(
    Task,
    input_names=[
        "nfiles", 
        "chunk_size", 
    ],
    optional_input_names=["index"],
    output_names=[
        "nfiles",
        "chunk_size",
        "chunk_range",
        "index", 
        "repeat",
        "trigger_compile",
        ],
):
    def run(self):
        self.outputs.nfiles = self.inputs.nfiles
        self.outputs.chunk_size = self.inputs.chunk_size

        if self.missing_inputs.index:
            index = 0
        else:
            index = self.inputs.index
        
        chunk_range = [index * self.inputs.chunk_size, (index + 1) * self.inputs.chunk_size]

        if chunk_range[0] == self.inputs.nfiles:
            self.outputs.repeat = False
            self.outputs.trigger_compile = True
        elif chunk_range[1] >= self.inputs.nfiles:
            self.outputs.repeat = False
            self.outputs.trigger_compile = True
        else:
            self.outputs.repeat = True
            self.outputs.trigger_compile = False
        self.outputs.chunk_range = chunk_range
        self.outputs.index = index + 1

# class Compile(
#     Task,
#     input_names=["trigger_compile"],
# ):
#     print("hola")

class ExecuteSubWorkflow(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "config"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine

        sub_graph = get_subworkflow(
            path_to_find=self.inputs.path_to_find,
            chunk_range=self.inputs.chunk_range,
            pattern=self.inputs.pattern,
            config=self.inputs.config,
        )

        execute_graph(
            graph=sub_graph,
            engine="dask",
        )

class ExecuteSubWorkflowSLURM(
    Task,
    input_names=["path_to_find", "chunk_range", "pattern", "config"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine, submitted to SLURM

        sub_graph = get_subworkflow(
            path_to_find=self.inputs.path_to_find,
            chunk_range=self.inputs.chunk_range,
            pattern=self.inputs.pattern,
            config=self.inputs.config,
        )

        kwargs = {}
        kwargs["_slurm_spawn_arguments"] = {
            "pre_script": "module load pyfai/2024.2",
            "parameters": {
                "time_limit": 360,
            },
        }

        # convert_graph(sub_graph, name_graph)
        # Now we have to submit this graph to slurm
        future = submit(args=(sub_graph,), kw=kwargs)
        future.get(timeout=None)


def get_subworkflow(path_to_find, chunk_range, pattern, config) -> dict:

    node_openintegratesave = {
        "id" : f"node_openintegratesave", 
        "task_type" : "class", 
        "task_identifier" : "tasks_config.OpenIntegrateSave",
        "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                            {"name" : "chunk_range", "value" : chunk_range},
                            {"name" : "pattern", "value" : pattern},
                            {"name" : "config", "value" : config},
                            ]
    }

    subgraph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [
            node_openintegratesave,
        ],
        "links" : [],
    }
    # convert_graph(subgraph, "subworkflow_config.json")
    return subgraph

def get_global_workflow(path_to_find, pattern, nfiles, chunk_size, config, slurm) -> dict:

    node_split = {
         "id" : "node_split", 
         "task_type" : "class", 
         "task_identifier" : "tasks_config.SplitList",
         "default_inputs" : [
             {"name" : "nfiles", "value" : nfiles},
             {"name" : "chunk_size", "value" : chunk_size},                             
            ]
    }

    # node_compile = {
    #      "id" : "node_compile", 
    #      "task_type" : "class", 
    #      "task_identifier" : "tasks_config.Compile",
    # }

    if slurm:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_config.ExecuteSubWorkflowSLURM",
            "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                                {"name" : "pattern", "value" : pattern},
                                {"name" : "config", "value" : config},
                                ]
        }
    else:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_config.ExecuteSubWorkflow",
            "default_inputs" : [{"name" : "path_to_find", "value" : path_to_find},
                                {"name" : "pattern", "value" : pattern},
                                {"name" : "config", "value" : config},
                                ]
        }
        

    link_self = {
        "source" : f"node_split",
        "target" : f"node_split",
        "data_mapping" : [
            {"source_output" : "nfiles", "target_input" : "nfiles"},
            {"source_output" : "chunk_size", "target_input" : "chunk_size"},
            {"source_output" : "index", "target_input" : "index"},
        ],
        "conditions" : [{"source_output": "repeat", "value": True}],
    }


    link_1 = {
            "source" : f"node_split",
            "target" : f"node_subworkflow",
            "data_mapping" : [
                {"source_output" : "chunk_range", "target_input" : "chunk_range"},
            ],
        }
    
    # link_compile = {
    #         "source" : f"node_split",
    #         "target" : f"node_compile",
    #         "data_mapping" : [
    #             {"source_output" : "trigger_compile", "target_input" : "trigger_compile"},
    #         ],
    # }
    
    graph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [
            node_split, 
            # node_compile, 
            node_subworkflow,
            ],
        "links" : [
            link_1, 
            link_self, 
            # link_compile,
            ],
        
    }
    # convert_graph(graph, "global_workflow_config.json")
    return graph

def execute_global_workflow(path_to_find, pattern, nfiles, chunk_size, config, slurm=True) -> None:

    global_workflow = get_global_workflow(
        path_to_find=path_to_find,
        pattern=pattern,
        nfiles=nfiles,
        chunk_size=chunk_size,
        config=config,
        slurm=slurm,
    )

    execute_graph(graph=global_workflow, engine="ppf")