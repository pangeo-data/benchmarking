from contextlib import contextmanager
from time import time, sleep
import pandas as pd
import yaml
from .datasets import timeseries
from .ops import global_mean, temporal_mean, climatology, anomaly
from distributed import wait
import datetime


class DiagnosticTimer:
    def __init__(self):
        self.diagnostics = []

    @contextmanager
    def time(self, **kwargs):
        tic = time()
        yield
        toc = time()
        kwargs['runtime'] = toc - tic
        self.diagnostics.append(kwargs)

    def dataframe(self):
        return pd.DataFrame(self.diagnostics)


def cluster_wait(client, n_workers):
    """ Delay process until all workers in the cluster are available.
    """
    start = time()
    wait_thresh = 300
    worker_thresh = n_workers * 0.95

    while len(client.cluster.scheduler.workers) < n_workers:
        sleep(2)
        elapsed = time() - start
        # If we are getting close to timeout but cluster is mostly available,
        # just break out
        if elapsed > wait_thresh and len(client.cluster.scheduler.workers) >= worker_thresh:
            break


class Setup:
    def __init__(self, input_file):
        try:
            with open(input_file) as f:
                self.params = yaml.safe_load(f)
        except Exception as exc:
            raise exc
        self.computations = [global_mean, temporal_mean, climatology, anomaly]
        self.client = None

    def cluster(self, num_nodes):
        from distributed import Client
        from dask_jobqueue import PBSCluster

        cluster_ = PBSCluster(walltime='00:15:00', cores=36, memory='60GB', processes=1)
        self.client = Client(cluster_)
        cluster_.scale(num_nodes)

    def run(self):

        for scaling_type, values in self.params.items():
            num_nodes = values['number_of_nodes']
            chsz = values['chunk_size']
            for num in num_nodes:
                timer = DiagnosticTimer()
                self.cluster(num)
                print(self.client)
                cluster_wait(self.client, num)
                for chunk_size in chsz:
                    ds = timeseries(chunk_size=chunk_size, n_workers=num).persist()
                    wait(ds)
                    for op in self.computations:
                        with timer.time(
                            operation=op.__name__,
                            chunk_size=chunk_size,
                            worker_per_node=1,
                            num_nodes=num,
                        ):
                            op(ds).compute()

                df = timer.dataframe()
                filename = f"results/compute_study_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M.%S')}_.csv"
                df.to_csv(filename, index=False)
                self.client.cluster.close()
