import datetime
import logging
import os
from contextlib import contextmanager
from time import sleep, time

import fsspec
import pandas as pd
from distributed import Client
from distributed.utils import format_bytes

from .datasets import timeseries
from .ops import (
    anomaly,
    climatology,
    deletefile,
    openfile,
    readfile,
    spatial_mean,
    temporal_mean,
    writefile,
)

logger = logging.getLogger()
logger.setLevel(level=logging.WARNING)


here = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
results_dir = os.path.join(here, 'results')


class DiagnosticTimer:
    def __init__(self):
        self.diagnostics = []

    @contextmanager
    def time(self, time_name, **kwargs):
        tic = time()
        yield
        toc = time()
        kwargs[time_name] = toc - tic
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


class Runner:
    def __init__(self, input_file):
        import yaml

        try:
            with open(input_file) as f:
                self.params = yaml.safe_load(f)
        except Exception as exc:
            raise exc
        self.operations = {}
        self.operations['computations'] = [spatial_mean, temporal_mean, climatology, anomaly]
        self.operations['readwrite'] = [writefile, openfile, readfile, deletefile]
        self.operations['write'] = [writefile]
        self.operations['read'] = [openfile, readfile]
        self.client = None

    def create_cluster(self, job_scheduler, maxcore, walltime, memory, queue, wpn):
        """ Creates a dask cluster using dask_jobqueue
        """
        logger.warning('Creating a dask cluster using dask_jobqueue')
        logger.warning(f'Job Scheduler: {job_scheduler}')
        logger.warning(f'Memory size for each node: {memory}')
        logger.warning(f'Number of cores for each node: {maxcore}')
        logger.warning(f'Number of workers for each node: {wpn}')

        from dask_jobqueue import PBSCluster, SLURMCluster

        job_schedulers = {'pbs': PBSCluster, 'slurm': SLURMCluster}

        # Note about OMP_NUM_THREADS=1, --threads 1:
        # These two lines are to ensure that each benchmark workers
        # only use one threads for benchmark.
        # in the job script one sees twice --nthreads,
        # but it get overwritten by --nthreads 1
        cluster = job_schedulers[job_scheduler](
            cores=maxcore,
            memory=memory,
            processes=wpn,
            local_directory='$TMPDIR',
            interface='ib0',
            queue=queue,
            walltime=walltime,
            env_extra=['OMP_NUM_THREADS=1'],
            extra=['--nthreads 1'],
            project='ntdd0004',
        )

        self.client = Client(cluster)

        logger.warning(
            '************************************\n'
            'Job script created by dask_jobqueue:\n'
            f'{cluster.job_script()}\n'
            '***************************************'
        )
        logger.warning(f'Dask cluster dashboard_link: {self.client.cluster.dashboard_link}')

    def run(self):
        logger.warning('Reading configuration YAML config file')
        operation_choice = self.params['operation_choice']
        machine = self.params['machine']
        job_scheduler = self.params['job_scheduler']
        queue = self.params['queue']
        walltime = self.params['walltime']
        maxmemory_per_node = self.params['maxmemory_per_node']
        maxcore_per_node = self.params['maxcore_per_node']
        chunk_per_worker = self.params['chunk_per_worker']
        freq = self.params['freq']
        spil = self.params['spil']
        output_dir = self.params.get('output_dir', results_dir)
        now = datetime.datetime.now()
        output_dir = os.path.join(output_dir, f'{machine}/{str(now.date())}')
        os.makedirs(output_dir, exist_ok=True)
        parameters = self.params['parameters']
        num_workers = parameters['number_of_workers_per_nodes']
        num_threads = parameters.get('number_of_threads_per_workers', 1)
        num_nodes = parameters['number_of_nodes']
        chunking_schemes = parameters['chunking_scheme']
        io_formats = parameters['io_format']
        filesystems = parameters['filesystem']
        fixed_totalsize = parameters['fixed_totalsize']
        chsz = parameters['chunk_size']
        writefile_dir = parameters['writefile_dir']
        for wpn in num_workers:
            self.create_cluster(
                job_scheduler=job_scheduler,
                maxcore=maxcore_per_node,
                walltime=walltime,
                memory=maxmemory_per_node,
                queue=queue,
                wpn=wpn,
            )
            for num in num_nodes:
                self.client.cluster.scale(num * wpn)
                cluster_wait(self.client, num * wpn)
                timer = DiagnosticTimer()
                # dfs = []
                logger.warning(
                    '#####################################################################\n'
                    f'Dask cluster:\n'
                    f'\t{self.client.cluster}\n'
                )
                now = datetime.datetime.now()
                csv_filename = f"{output_dir}/compute_study_{now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
                for chunk_size in chsz:

                    for io_format in io_formats:

                        for filesystem in filesystems:

                            if filesystem == 's3':
                                profile = parameters['profile']
                                bucket = parameters['bucket']
                                endpoint_url = parameters['endpoint_url']
                                fs = fsspec.filesystem(
                                    's3',
                                    profile=profile,
                                    anon=False,
                                    client_kwargs={'endpoint_url': endpoint_url},
                                )
                                root = f'{bucket}/test1'
                            elif filesystem == 'posix':
                                fs = None
                                root = writefile_dir
                                if not os.path.isdir(f'{root}/test1'):
                                    os.makedirs(f'{root}/test1')
                            for chunking_scheme in chunking_schemes:

                                logger.warning(
                                    f'Benchmark starting with: \n\tworker_per_node = {wpn},'
                                    f'\n\tnum_nodes = {num}, \n\tchunk_size = {chunk_size},'
                                    f'\n\tchunking_scheme = {chunking_scheme},'
                                    f'\n\tchunk per worker = {chunk_per_worker}'
                                    f'\n\tio_format = {io_format}'
                                    f'\n\tfilesystem = {filesystem}'
                                )
                                ds, chunks = timeseries(
                                    fixed_totalsize=fixed_totalsize,
                                    chunk_per_worker=chunk_per_worker,
                                    chunk_size=chunk_size,
                                    chunking_scheme=chunking_scheme,
                                    io_format=io_format,
                                    num_nodes=num,
                                    freq=freq,
                                    worker_per_node=wpn,
                                )
                                # wait(ds)
                                dataset_size = format_bytes(ds.nbytes)
                                logger.warning(ds)
                                logger.warning(f'Dataset total size: {dataset_size}')

                                for op in self.operations[operation_choice]:
                                    with timer.time(
                                        'runtime',
                                        operation=op.__name__,
                                        fixed_totalsize=fixed_totalsize,
                                        chunk_size=chunk_size,
                                        chunk_per_worker=chunk_per_worker,
                                        dataset_size=dataset_size,
                                        worker_per_node=wpn,
                                        threads_per_worker=num_threads,
                                        num_nodes=num,
                                        chunking_scheme=chunking_scheme,
                                        io_format=io_format,
                                        filesystem=filesystem,
                                        root=root,
                                        machine=machine,
                                        maxmemory_per_node=maxmemory_per_node,
                                        maxcore_per_node=maxcore_per_node,
                                        spil=spil,
                                    ):
                                        fname = f'{chunk_size}{chunking_scheme}{filesystem}{num}'
                                        if op.__name__ == 'writefile':
                                            print(ds.sst.data.chunksize)
                                            filename = op(ds, fs, io_format, root, fname)
                                        elif op.__name__ == 'openfile':
                                            ds = op(fs, io_format, root, chunks, chunk_size)
                                        elif op.__name__ == 'deletefile':
                                            ds = op(fs, io_format, root, filename)
                                        else:
                                            op(ds)
                        # kills ds, and every other dependent computation
                        logger.warning('Computation done')
                        self.client.cancel(ds)
                        temp_df = timer.dataframe()
                        temp_df.to_csv(csv_filename, index=False)
                        # dfs.append(temp_df)

                # now = datetime.datetime.now()
                # filename = f"{output_dir}/compute_study_{now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
                # df = pd.concat(dfs)
                # df.to_csv(filename, index=False)
                logger.warning(f'Persisted benchmark result file: {csv_filename}')

            logger.warning(
                'Shutting down the client and cluster before changing number of workers per nodes'
            )
            self.client.cluster.close()
            logger.warning('Cluster shutdown finished')
            self.client.close()
            logger.warning('Client shutdown finished')

        logger.warning('=====> The End <=========')
