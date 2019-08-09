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
from abc import ABC, abstractmethod
from distributed import Client


#warnings.simplefilter('ignore')  # Silence warnings
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


class AbstractSetup(ABC):
    def __init__(self, input_file):
        try:
            with open(input_file) as f:
                self.params = yaml.safe_load(f)
        except Exception as exc:
            raise exc
        self.computations = [global_mean, temporal_mean, climatology, anomaly]
        self.client = None

    @abstractmethod
    def create_cluster(self):
        """ Creates a dask cluster using dask_jobqueue
        """
        pass

    def run(self, verbose=False):
        if verbose:
            print ('read configuration yaml file here to run the benchmarks ')
        machine = self.params['machine']
        #machine_specifications = self.params['machine_specifications']
        queue = self.params['queue']
        walltime = self.params['walltime']
        maxmemory_per_node = self.params['maxmemory_per_node']
        maxcore_per_node = self.params['maxcore_per_node']
        spil = self.params['spil']
        output_dir = self.params['output_dir']
        os.makedirs(output_dir, exist_ok=True)
        parameters = self.params['parameters']
        num_workers = parameters['number_of_workers_per_nodes']
        num_nodes = parameters['number_of_nodes']
        chunking_schemes = parameters['chunking_scheme']
        chsz = parameters['chunk_size']

        for wpn in num_workers:
        #for wpn in range(1,maxcore_per_node,step_core):
            worker_per_node=wpn
            self.create_cluster(maxcore=maxcore_per_node,walltime=walltime,memory=maxmemory_per_node,queue=queue,wpn=wpn,verbose=verbose)
            for num in num_nodes:
                #self.client.cluster.scale(num )
                self.client.cluster.scale(num * wpn)
                if verbose:
                    print('start cluster_wait')
                #cluster_wait(self.client, num )
                cluster_wait(self.client, num * wpn)
                timer = DiagnosticTimer()
                dfs = []
                if verbose:
                    print( f'dask cluster client started  with {num} nodes')
                    print( 'client cluster ')
                    print(len(self.client.cluster.scheduler.workers))
                for chunk_size in chsz:

                    for chunking_scheme in chunking_schemes:
                        if verbose:
                            print(
                                f'benchmark start with: worker_per_node={wpn}, num_nodes={num}, chunk_size={chunk_size}, chunking_scheme={chunking_scheme}'
                            )
                        ds = timeseries(
                            chunk_size=chunk_size,
                            chunking_scheme=chunking_scheme,
                            num_nodes=num,
                            worker_per_node=wpn,
                        ).persist()
                        wait(ds)
                        dataset_size = format_bytes(ds.nbytes)
                        if verbose:
                            print(ds)
                            print('\n datasize: %.1f GB' %(ds.nbytes / 1e9))
                        for op in self.computations:
                            with timer.time(
                                operation=op.__name__,
                                chunk_size=chunk_size,
                                dataset_size=dataset_size,
                                worker_per_node=wpn,
                                num_nodes=num,
                                chunking_scheme=chunking_scheme,
                                machine=machine,
                                maxmemory_per_node=maxmemory_per_node,
                                maxcore_per_node=maxcore_per_node,
                                spil=spil,
                            ):
                                wait(op(ds).persist())
                        if verbose:
                            print('kills ds and every other dependent computation')
                            #print('restarting all worker process before starting series of bench')
                        #self.client.restart()  # hard restart of all worker processes.
                        self.client.cancel(ds)  # kills ds, and every other dependent computation
                    temp_df = timer.dataframe()
                    dfs.append(temp_df)

                filename = f"{output_dir}/compute_study_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M.%S')}_.csv"
                if verbose:
                    print('create bench mark result file: ',filename)
                df = pd.concat(dfs)
                df.to_csv(filename, index=False)

            print('client and cluster close before changing number of workers per nodes')
            self.client.cluster.close()
            print('client cluster close finished')
            self.client.close()
            print('client  close finished')


class PBSSetup(AbstractSetup):
    def create_cluster(self, maxcore, walltime, memory, queue,wpn,verbose):
    #def create_cluster(self, worker_per_node):
        if verbose:
            print( """ Creates a dask cluster using dask_jobqueue """)
            print( 'memory size for each node: ', memory)
            print( 'number of cores for each node: ', maxcore)
            print( 'number of workers for each node: ', wpn)
            #print( 'to do here, write somewhere the distributed spil config, bandwidth etc ?')
            

        from dask_jobqueue import PBSCluster
        cluster = PBSCluster(
            cores=maxcore,
            memory=memory,
            processes=wpn,
            local_directory='$TMPDIR',
            interface='ib0',
            queue=queue,
            walltime=walltime,
            job_extra=["-j oe"],
        )
       #     extra=['--memory-target-fraction 0.95', '--memory-pause-fraction 0.9']
        self.client = Client(cluster)
        if verbose:
            print( 'job script created by dask_jobqueue: starts--------')
            print(cluster.job_script())
            print( 'job script created by dask_jobqueue: ends --------')
            #print(cluster.job_file())
            print( 'dask cluster dashboard_link : ' )
            print(self.client.cluster.dashboard_link)