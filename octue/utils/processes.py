class ProcessesContextManager:
    def __init__(self, processes):
        self.processes = processes

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        for process in self.processes:
            process.kill()
