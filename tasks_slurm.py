



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
