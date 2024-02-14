
from ewoks import execute_graph, convert_graph



def generate_integration_workflow(filename_list, index_tag="", convert=True):

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
    if convert:
        convert_graph(graph, f"workflow_{index_tag}.json")

    return graph





def generate_chunks_edf_integration(filename_list, mask_file, poni_file, chunk_size=5, engine="ppf"):
    for index in range(int(len(filename_list)/chunk_size) + 1):
        chunk_range = [index * chunk_size, (index + 1) * chunk_size]
        chunk = filename_list[chunk_range[0]:chunk_range[1]]

        graph = generate_integration_workflow(filename_list=chunk, index_tag=str(index), convert=True)

        execute_graph(
            graph=graph,
            engine=engine,
            inputs=[
                {"name" : "edf_filename", "value" : mask_file, "id" : "node_open_mask"},
                {"name" : "filename", "value" : poni_file, "id" : "node_config"},
            ],
        )