
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
from silx.io import h5py_utils
import numpy as np


def generate_workflow_dummy(execute=True):
    node_dummy = {"id" : "node_dummy", "task_type" : "class", "task_identifier" : "tasks_config_h5.Write"}
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


class OpenIntegrateSave(Task, input_names=["h5_file", "scan_number", "detector_name", "chunk_range", "config"]):
     def run(self):
        h5_file = self.inputs.h5_file
        scan_number = self.inputs.scan_number
        chunk_range = [int(_) for _ in self.inputs.chunk_range]
        detector_name = self.inputs.detector_name

        with open(self.inputs.config) as fp:
            config = json.load(fp)

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


        # Read
        with h5py_utils.open_item(h5_file, "/", mode="a") as f:
            dataset_chunk = f[scan_number]["measurement"][detector_name][chunk_range[0]:chunk_range[1]]
            for data in dataset_chunk:
                res = ai.integrate1d(
                    data=data,
                    npt=config["nbpt_rad"],
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
                pyfai_group = f[scan_number].create_group("pyFAI_integration")
                data_res = np.array(res.radial, res.intensity)
                pyfai_group.create_dataset(data=data_res, name="integration")
               
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
        elif chunk_range[1] >= self.inputs.nfiles:
            self.outputs.repeat = False
        else:
            self.outputs.repeat = True
        self.outputs.chunk_range = chunk_range
        self.outputs.index = index + 1

class ExecuteSubWorkflow(
    Task,
    input_names=["h5_file", "scan_number", "detector_name", "chunk_range", "config"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine

        sub_graph = get_subworkflow(
            h5_file=self.inputs.h5_file,
            scan_number=self.inputs.scan_number,
            detector_name=self.inputs.detector_name,
            chunk_range=self.inputs.chunk_range,
            config=self.inputs.config,
        )

        execute_graph(
            graph=sub_graph,
            engine="dask",
        )

class ExecuteSubWorkflowSLURM(
    Task,
    input_names=["h5_file", "scan_number", "detector_name", "chunk_range", "config"],
):
    def run(self):
        # Execute a sub-workflow using Dask engine, submitted to SLURM

        sub_graph = get_subworkflow(
            h5_file=self.inputs.h5_file,
            scan_number=self.inputs.scan_number,
            detector_name=self.inputs.detector_name,
            chunk_range=self.inputs.chunk_range,
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


def get_subworkflow(h5_file, scan_number, detector_name, chunk_range, config) -> dict:

    node_openintegratesave = {
        "id" : f"node_openintegratesave", 
        "task_type" : "class", 
        "task_identifier" : "tasks_config_h5.OpenIntegrateSave",
        "default_inputs" : [{"name" : "h5_file", "value" : h5_file},
                            {"name" : "scan_number", "value" : scan_number},
                            {"name" : "detector_name", "value" : detector_name},
                            {"name" : "chunk_range", "value" : chunk_range},
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

def get_global_workflow(h5_file, scan_number, detector_name, nfiles, chunk_size, config, slurm) -> dict:

    node_split = {
         "id" : "node_split", 
         "task_type" : "class", 
         "task_identifier" : "tasks_config_h5.SplitList",
         "default_inputs" : [
             {"name" : "nfiles", "value" : nfiles},
             {"name" : "chunk_size", "value" : chunk_size},                             
            ]
    }
    
    if slurm:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_config_h5.ExecuteSubWorkflowSLURM",
            "default_inputs" : [{"name" : "h5_file", "value" : h5_file},
                                {"name" : "scan_number", "value" : scan_number},
                                {"name" : "detector_name", "value" : detector_name},
                                {"name" : "config", "value" : config},
                                ]
        }
    else:
        node_subworkflow = {
            "id" : "node_subworkflow", 
            "task_type" : "class", 
            "task_identifier" : "tasks_config_h5.ExecuteSubWorkflow",
            "default_inputs" : [{"name" : "h5_file", "value" : h5_file},
                                {"name" : "scan_number", "value" : scan_number},
                                {"name" : "detector_name", "value" : detector_name},
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
    
    graph = {
        "graph" : {"id" : f"subgraph"},
        "nodes" : [node_split, node_subworkflow],
        "links" : [link_1, link_self],
        
    }
    # convert_graph(graph, "global_workflow_config.json")
    return graph

def execute_global_workflow(h5_file, scan_number, detector_name, nfiles, chunk_size, config, slurm=True) -> None:

    global_workflow = get_global_workflow(
        h5_file=h5_file,
        scan_number=scan_number,
        detector_name=detector_name,
        nfiles=nfiles,
        chunk_size=chunk_size,
        config=config,
        slurm=slurm,
    )

    execute_graph(graph=global_workflow, engine="ppf")