import os

import dask.array as da
import fsspec
import h5py
import numpy as np
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


def openfile(fs, io_format, root):
    if isinstance(fs, fsspec.AbstractFileSystem):
        if io_format == 'zarr':
            # ds = xr.open_zarr(fs.get_mapper(f'{root}/sst.zarr'), consolidated=True)
            ds = da.from_zarr(fs.get_mapper(f'{root}/sst.zarr/sst'))
        elif io_format == 'netcdf':
            fileObj = fs.open(f'{root}/test1/sst.nc')
            print(f'{root}/sst.nc')
            d = xr.open_dataset(fileObj, engine='h5netcdf')
            ds = da.from_array(d['sst'])
    else:
        if io_format == 'zarr':
            ds = da.from_zarr('test1/sst.zarr/sst')
        elif io_format == 'netcdf':
            f = h5py.File('test1/sst.nc', 'r')
            ds = da.from_array(f['sst'])

    return ds


def writefile(ds, fs, io_format, root):
    if isinstance(fs, fsspec.AbstractFileSystem):
        if io_format == 'zarr':
            store = fs.get_mapper(root=f'{root}/sst.zarr', check=False, create=True)
            ds = ds.to_zarr(store, consolidated=True, compute=False, mode='w')
            ds.compute()
        elif io_format == 'netcdf':

            # store = f'test1/sst.nc'
            # ds = ds.to_netcdf(store, engine='h5netcdf', compute=False, mode='w')
            # ds.compute()
            fs.upload(lpath=f'test1', rpath=f'{root}/', recursive=True)

            # print(fs.ls(f'{root}'))
            print(root)
            print(fs.ls(f'{root}/test1'))
            print(fs.du(f'{root}'))
            print(fs.du(f'{root}/test1'))
            # fileObj = fs.open(path=f'{root}/sst.nc')
            # ds = xr.open_dataset(fileObj, engine='h5netcdf')

    else:
        print(io_format)
        if io_format == 'zarr':
            store = f'test1/sst.zarr'
            ds = ds.to_zarr(store=store, consolidated=True, compute=False, mode='w')
        elif io_format == 'netcdf':
            store = f'test1/sst.nc'
            ds = ds.to_netcdf(store, engine='h5netcdf', compute=False, mode='w')
        ds.compute()

    return ds


def deletefile(fs, io_format, root):
    if isinstance(fs, fsspec.AbstractFileSystem):
        if io_format == 'zarr':
            ret = fs.rm(path=f'{root}/sst.zarr', recursive=True)
        elif io_format == 'netcdf':
            ret = fs.rm(path=f'{root}/test1/sst.nc')
    else:
        if io_format == 'zarr':
            ret = os.rmdir('sst.zarr')
        elif io_format == 'netcdf':
            ret = os.remove('sst.nc')
    return ret
