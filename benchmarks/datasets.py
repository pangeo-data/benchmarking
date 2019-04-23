import numpy as np
import dask.array as da
import xarray as xr
from distributed.utils import parse_bytes
import math
import pandas as pd


def timeseries(
    chunk_size='128 MB',
    n_workers=1,
    chunk_over_time_dim=True,
    lat=320,
    lon=384,
    start='1980-01-01',
    freq='1D',
):
    """ Create synthetic Xarray dataset filled with random
    data.

    Parameters
    ----------
    chunk_size : str
          chunk size in bytes, kilo, mega or any factor of bytes
    n_workers : int
           number of dask workers
    chunk_over_time_dim : bool, default True
           Whether to chunk across time dimension or horizontal dimensions (lat, lon)
    lat : int
         number of latitude values

    lon : int
         number of longitude values

    start : datetime (or datetime-like string)
        Start of time series

    freq : string
        String like '2s' or '1H' or '12W' for the time series frequency


    Examples
    ---------

    >>> from benchmarks.datasets import timeseries
    >>> ds = timeseries()
    >>> ds
    <xarray.Dataset>
    Dimensions:  (lat: 320, lon: 384, time: 131)
    Coordinates:
    * time     (time) datetime64[ns] 1980-01-31 1980-02-29 ... 1990-11-30
    * lon      (lon) float64 0.0 0.9399 1.88 2.82 3.76 ... 357.2 358.1 359.1 360.0
    * lat      (lat) float64 0.0 0.5643 1.129 1.693 ... 178.3 178.9 179.4 180.0
    Data variables:
        sst      (time, lon, lat) float64 dask.array<shape=(131, 384, 320), chunksize=(109, 384, 320)>
    """

    dt = np.dtype('f8')
    itemsize = dt.itemsize
    chunk_size = parse_bytes(chunk_size)
    total_bytes = chunk_size * n_workers
    size = total_bytes / itemsize
    timesteps = math.ceil(size / (lat * lon))
    shape = (timesteps, lon, lat)
    if chunk_over_time_dim:
        x = math.ceil(chunk_size / (lon * lat * itemsize))
        chunks = (x, lon, lat)
    else:
        x = math.ceil(math.sqrt(chunk_size / (timesteps * itemsize)))
        chunks = (timesteps, x, x)

    lats = xr.DataArray(np.linspace(start=-90, stop=90, num=lat), dims=['lat'])
    lons = xr.DataArray(np.linspace(start=-180, stop=180, num=lon), dims=['lon'])
    times = xr.DataArray(pd.date_range(start=start, freq=freq, periods=timesteps), dims=['time'])
    random_data = randn(shape=shape, chunks=chunks)
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


def randn(shape, chunks=None, seed=0):
    rng = da.random.RandomState(seed)
    x = 5 + 3 * rng.standard_normal(shape, chunks=chunks)
    return x
