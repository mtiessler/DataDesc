class BaseProfiler:
    name = "base"

    def run(self, ctx):
        raise NotImplementedError("Profiler must implement run(ctx)")
