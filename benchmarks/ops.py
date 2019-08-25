# def writedown_zarr
# def writedown_netcdf( create yeary netcdf files)
# def mean operations ( global, temporal) with reading netcdf data
# def mean operations ( global , temporal) with reading zarr file



def global_mean(ds):
    return ds.mean(dim='time')
# add plots here?and save it as png or such? 
# if we bench pangeo as interactive hoc post processing tool
# creating plot = analysis 
# should be added ?

def temporal_mean(ds):
    return ds.mean(dim=['lat', 'lon'])


def climatology(ds):
    seasonal_clim = ds.groupby('time.season').mean(dim='time')
    return seasonal_clim


def anomaly(ds):
    seasonal_clim = climatology(ds)
    seasonal_anom = ds.groupby('time.season') - seasonal_clim
    return seasonal_anom
