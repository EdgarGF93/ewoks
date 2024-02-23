from ewokscore import Task




class CompileEDF(
    Task,
    input_names=["res_dict", "res_list", "nfiles"],
    output_names=["res_dict", "res_list"]
)