from contextlib import contextmanager
from time import time, sleep
import pandas as pd
import yaml
from .datasets import timeseries
from .ops import global_mean, temporal_mean, climatology, anomaly
from distributed import wait
from distributed.utils import format_bytes
import datetime
import warnings

warnings.simplefilter('ignore')  # Silence warnings
import os


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
    wait_thresh = 600
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

    def create_cluster(self, worker_per_node):
        """ Creates a dask cluster using dask_jobqueue
        """
        from distributed import Client
        from dask_jobqueue import PBSCluster

        cluster = PBSCluster(
            walltime='00:30:00',
            cores=worker_per_node,
            memory='109GB',
            processes=worker_per_node,
            queue='regular',
        )
        self.client = Client(cluster)

    def run(self, verbose=False):
        """ Runs the benchmarks using configurations from a YAML file
        """
        machine = self.params['machine']
        output_dir = self.params['output_dir']
        os.makedirs(output_dir, exist_ok=True)
        parameters = self.params['parameters']
        num_nodes = parameters['number_of_nodes']
        worker_per_node = parameters['worker_per_node']
        chsz = parameters['chunk_size']
        for wpn in worker_per_node:
            self.create_cluster(worker_per_node=wpn)
            dfs = []
            for num in num_nodes:
                self.client.cluster.scale(num * wpn)
                cluster_wait(self.client, num * wpn)
                timer = DiagnosticTimer()
                if verbose:
                    print(self.client.cluster)
                    print(self.client.cluster.dashboard_link)
                for chunk_size in chsz:
                    if verbose:
                        print(f'worker_per_node={wpn}, num_nodes={num}, chunk_size={chunk_size}')
                    ds = timeseries(
                        chunk_size=chunk_size, num_nodes=num, worker_per_node=wpn
                    ).persist()
                    wait(ds)
                    dataset_size = format_bytes(ds.nbytes)
                    for op in self.computations:
                        with timer.time(
                            operation=op.__name__,
                            chunk_size=chunk_size,
                            dataset_size=dataset_size,
                            worker_per_node=wpn,
                            num_nodes=num,
                        ):
                            op(ds).compute()

                    self.client.cancel(ds)  # kills ds, and every other dependent computation
                temp_df = timer.dataframe()
                dfs.append(temp_df)
                self.client.restart()  # hard restart of all worker processes.

            filename = f"{output_dir}/compute_study_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M.%S')}_.csv"
            df = pd.concat(dfs)
            df.to_csv(filename, index=False)
            self.client.cluster.close()
            self.client.close()
