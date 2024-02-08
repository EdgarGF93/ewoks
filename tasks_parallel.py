from ewokscore.task import Task

class SplitFrames(
        Task,
        input_names=["h5file"],
        optional_input_names=["index_frame"],
        output_names=["frame"],
    ):
    def run(self):
        pass

class PrintFrame(
        Task,
        input_names=["frame"],
    ):
    def run(self):
        print(self.inputs.frame)
