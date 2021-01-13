import itertools
import os
import shutil

import dask.array as da
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
    null_store = DevNullStore()
    null_store['foo'] = 'bar'
    future = da.store(ds, null_store, lock=False, compute=False)
    future.compute()


def delete_dir(dir):
    files = os.listdir(dir)
    for file in files:
        os.remove(os.path.join(dir, file))


def get_filelist(fs, io_format, root, chunk_size):
    if io_format == 'zarr':
        fileObjs = fs.glob(f'{root}/sst.{chunk_size}*.zarr')
    elif (io_format == 'netcdf') & (fs.protocol[0] == 's3'):
        # subs = 'zarr'
        # zarr_flist = list(filter(lambda x: subs in x, fs.glob(f'{root}/*')))
        fileObjs = [fs.open(p) for p in fs.glob(f'{root}/sst.{chunk_size}*.nc')]

    return fileObjs


def openfile(fs, io_format, root, chunks, chunk_size):
    if io_format == 'zarr':
        fileObjs = fs.glob(f'{root}/sst.{chunk_size}*.zarr')
        print(fileObjs)
        f = xr.open_zarr(fs.get_mapper(f'{fileObjs[0]}'))
        ds = f.sst.data
    elif (io_format == 'netcdf') & (fs.protocol[0] == 's3'):
        fileObjs = [fs.open(p) for p in fs.glob(f'{root}/sst.{chunk_size}*.nc')]
        print(fileObjs)
        datasets = [xr.open_dataset(p, chunks={'time': chunks[0]}) for p in fileObjs]
        f = xr.concat(datasets, dim='time')
        ds = f.sst.data
    elif (io_format == 'netcdf') & (fs.protocol == 'file'):
        f = xr.open_mfdataset(f'{root}/sst.*.nc', combine='by_coords', chunks={'time': chunks[0]})
        ds = f.sst.data

    return ds


def writefile(ds, fs, io_format, root, fname):
    filename = f'sst.{fname}'
    if io_format == 'zarr':
        store = fs.get_mapper(root=f'{root}/{filename}.zarr', check=False, create=True)
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
        paths = [create_filepath(ds, prefix=filename, root_path=f'{root}') for ds in dss]
        xr.save_mfdataset(datasets=dss, paths=paths)
        if fs.protocol[0] == 's3':
            fs.upload(lpath=f'{root}', rpath=f'{root}/', recursive=True)

    return filename


def deletefile(fs, io_format, root, filename):
    if fs.protocol[0] == 's3':
        if io_format == 'zarr':
            ret = fs.rm(path=f'{root}/{filename}.zarr', recursive=True)
        elif io_format == 'netcdf':
            ret = delete_dir('test1')
            ret = fs.rm(path=f'{root}', recursive=True)
    else:
        ret = shutil.rmtree(f'{root}')
    return ret


def split_by_chunks(dataset):
    """
    COPIED from https://github.com/pydata/xarray/issues/1093#issuecomment-259213382
    """
    chunk_slices = {}
    for dim, chunks in dataset.chunks.items():
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
