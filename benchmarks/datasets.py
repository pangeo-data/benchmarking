import numpy as np
import dask.array as da
import xarray as xr
from distributed.utils import parse_bytes
import math
import pandas as pd
import dask


def timeseries(
    chunk_per_worker=10,
    chunk_size='128 MB',
    num_nodes=1,
    worker_per_node=1,
    chunking_scheme=None,
    lat=320,
    lon=384,
    start='1980-01-01',
    freq='1D',
    nan=False,
):
    """ Create synthetic Xarray dataset filled with random
    data.

    Parameters
    ----------
    chunk_per_worker : int 
          number of chunk placed per worker.  
          see docs.dask.org, best practices, for chunk.  Best chunk size is around 100M but, each worker can have many chunk, which automate the parallelism in dask.  
    chunk_size : str
          chunk size in bytes, kilo, mega or any factor of bytes
    num_nodes : int
           number of compute nodes
    worker_per_node: int
           number of dask workers per node

    chunking_scheme : str
           Whether to chunk across time dimension ('temporal') or horizontal dimensions (lat, lon) ('spatial').
           If None, automatically determine chunk sizes along all dimensions.

    lat : int
         number of latitude values

    lon : int
         number of longitude values

    start : datetime (or datetime-like string)
        Start of time series

    freq : string
        String like '2s' or '1H' or '12W' for the time series frequency
    nan : bool
         Whether to include nan in generated data


    Examples
    ---------

    >>> from benchmarks.datasets import timeseries
    >>> ds = timeseries('128MB', 5, chunking_scheme='spatial', lat=500, lon=600)
    >>> ds
    <xarray.Dataset>
    Dimensions:  (lat: 500, lon: 600, time: 267)
    Coordinates:
    * time     (time) datetime64[ns] 1980-01-01 1980-01-02 ... 1980-09-23
    * lon      (lon) float64 -180.0 -179.4 -178.8 -178.2 ... 178.8 179.4 180.0
    * lat      (lat) float64 -90.0 -89.64 -89.28 -88.92 ... 88.92 89.28 89.64 90.0
    Data variables:
        sst      (time, lon, lat) float64 dask.array<shape=(267, 600, 500), chunksize=(267, 245, 245)>
    Attributes:
        history:  created for compute benchmarking
    """

    dt = np.dtype('f8')
    itemsize = dt.itemsize
    chunk_size = parse_bytes(chunk_size)
    total_bytes = chunk_size * num_nodes * worker_per_node * chunk_per_worker
    #total_bytes = chunk_size * num_nodes * worker_per_node 
    size = total_bytes / itemsize
    timesteps = math.ceil(size / (lat * lon))
    shape = (timesteps, lon, lat)
    if chunking_scheme == 'temporal':
        x = math.ceil(chunk_size / (lon * lat * itemsize))
        chunks = (x, lon, lat)
    elif chunking_scheme == 'spatial':
        x = math.ceil(math.sqrt(chunk_size / (timesteps * itemsize)))
        chunks = (timesteps, x, x)
    else:
        chunks = 'auto'

    lats = xr.DataArray(np.linspace(start=-90, stop=90, num=lat), dims=['lat'])
    lons = xr.DataArray(np.linspace(start=-180, stop=180, num=lon), dims=['lon'])
    times = xr.DataArray(pd.date_range(start=start, freq=freq, periods=timesteps), dims=['time'])
    if chunks == 'auto':
        with dask.config.set({'array.chunk-size': chunk_size}):
            random_data = randn(shape=shape, chunks=chunks, nan=nan)
    else:
        random_data = randn(shape=shape, chunks=chunks, nan=nan)
    ds = xr.DataArray(
        random_data,
        dims=['time', 'lon', 'lat'],
        coords={'time': times, 'lon': lons, 'lat': lats},
        name='sst',
        encoding=None,
        attrs={'units': 'baz units', 'description': 'a description'},
    ).to_dataset()
    ds.attrs = {'history': 'created for compute benchmarking'}

    return ds


def randn(shape, chunks=None, nan=False, seed=0):
    rng = da.random.RandomState(seed)
    x = 5 + 3 * rng.standard_normal(shape, chunks=chunks)
    if nan:
        x = da.where(x < 0, np.nan, x)
    return x
