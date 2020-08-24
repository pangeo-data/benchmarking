import itertools
import os
from pathlib import Path

import dask.array as da
import fsspec
import numpy as np
import pandas as pd
import xarray as xr


class DevNullStore:
    def __init__(self):
        pass

    def __setitem__(*args, **kwargs):
        pass


def temporal_mean(ds):
    return ds.mean(dim='time')


def spatial_mean(ds):
    weights = np.cos(np.radians(ds.lat)).where(ds['sst'][0].notnull())
    weights /= weights.sum()
    return (ds * weights).mean(dim=['lat', 'lon'])


def climatology(ds):
    seasonal_clim = ds.groupby('time.season').mean(dim='time')
    return seasonal_clim


def anomaly(ds):
    seasonal_clim = climatology(ds)
    seasonal_anom = ds.groupby('time.season') - seasonal_clim
    return seasonal_anom


def readfile(ds):
    print('readfile')
    null_store = DevNullStore()
    null_store['foo'] = 'bar'
    future = da.store(ds, null_store, lock=False, compute=False)
    future.compute()


def delete_dir(dir):
    files = os.listdir(dir)
    for file in files:
        os.remove(os.path.join(dir, file))


def openfile(fs, io_format, root, chunks, chunk_size):
    print('openfile')
    print(chunks)
    if isinstance(fs, fsspec.AbstractFileSystem):
        if io_format == 'zarr':
            f = fs.glob(f'{root}/sst.{chunk_size}*.zarr')
            ds = da.from_zarr(fs.get_mapper(f'{f[0]}/sst'))
            print(ds)
        elif io_format == 'netcdf':
            subs = 'zarr'
            zarr_flist = list(filter(lambda x: subs in x, fs.glob(f'{root}/*')))
            fileObjs = [fs.open(p) for p in fs.ls(f'{root}/') if p not in zarr_flist]
            print(list(filter(lambda x: 'nc' in x, fs.glob(f'{root}/*'))))
            # datasets = [xr.open_dataset(p, engine='h5netcdf', chunks={'time': chunks[0][0]}) for p in fileObjs]
            datasets = [
                xr.open_dataset(p, engine='h5netcdf', chunks={'time': chunks[0]}) for p in fileObjs
            ]
            f = xr.concat(datasets, dim='time')
            ds = f.sst.data
            print(ds)
    else:
        if io_format == 'zarr':
            f = Path(f'{root}').glob(f'test1/sst.{chunk_size}*.zarr')
            ds = da.from_zarr(f'{next(f).as_posix()}/sst')
            print(ds)
        elif io_format == 'netcdf':
            f = xr.open_mfdataset(
                f'{root}/test1/sst.*.nc', combine='by_coords', chunks={'time': chunks[0]}
            )
            ds = f.sst.data
            print(ds)

    return ds


def writefile(ds, fs, io_format, root, fname):
    filename = f'sst.{fname}'
    # if isinstance(fs, fsspec.AbstractFileSystem):
    if io_format == 'zarr':
        if isinstance(fs, fsspec.AbstractFileSystem):
            store = fs.get_mapper(root=f'{root}/{filename}.zarr', check=False, create=True)
        else:
            store = f'{root}/test1/{filename}.zarr'
        ds = ds.to_zarr(
            store,
            encoding={'sst': {'compressor': None}},
            consolidated=True,
            compute=False,
            mode='w',
        )
        ds.compute()
    elif io_format == 'netcdf':
        ds_list = list(split_by_chunks(ds))
        dss = [item[1] for item in ds_list]
        paths = [create_filepath(ds, prefix=filename, root_path=f'{root}/test1') for ds in dss]
        xr.save_mfdataset(datasets=dss, paths=paths)
        if isinstance(fs, fsspec.AbstractFileSystem):
            fs.upload(lpath=f'{root}/test1', rpath=f'{root}/', recursive=True)

    return filename


def deletefile(fs, io_format, root, filename):
    if isinstance(fs, fsspec.AbstractFileSystem):
        if io_format == 'zarr':
            ret = fs.rm(path=f'{root}/{filename}.zarr', recursive=True)
            # ret = fs.rm(path=f'{root}', recursive=True)
        elif io_format == 'netcdf':
            ret = delete_dir('test1')
            ret = fs.rm(path=f'{root}/test1', recursive=True)
    else:
        if io_format == 'zarr':
            ret = os.system(f'rm -rf {root}/test1')
            # ret = delete_dir('test1')
        elif io_format == 'netcdf':
            ret = delete_dir(f'{root}/test1')
    return ret


def split_by_chunks(dataset):
    """
    COPIED from https://github.com/pydata/xarray/issues/1093#issuecomment-259213382
    """
    chunk_slices = {}
    for dim, chunks in dataset.chunks.items():
        print(dim)
        print(dataset.sizes[dim])
        slices = []
        start = 0
        if len(chunks) > 10:
            chunk_range = int(len(chunks) / 10)
        else:
            chunk_range = 1
        for i in range(len(chunks) - chunk_range + 1):
            if start >= dataset.sizes[dim]:
                break
            stop = start + chunks[i] * chunk_range
            slices.append(slice(start, stop))
            print(start, stop)
            start = stop
        chunk_slices[dim] = slices
    for slices in itertools.product(*chunk_slices.values()):
        selection = dict(zip(chunk_slices.keys(), slices))
        yield (selection, dataset[selection])


def create_filepath(ds, prefix='filename', root_path='.'):
    """
    Generate a filepath when given an xarray dataset
    """
    start = pd.to_datetime(str(ds.time.data[0])).strftime('%Y-%m-%d')
    end = pd.to_datetime(str(ds.time.data[-1])).strftime('%Y-%m-%d')
    filepath = f'{root_path}/{prefix}_{start}_{end}.nc'
    return filepath
