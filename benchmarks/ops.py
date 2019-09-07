import numpy as np


def temporal_mean(ds):
    return ds.mean(dim="time")


def spatial_mean(ds):
    weights = np.cos(np.radians(ds.lat)).where(ds["sst"][0].notnull())
    weights /= weights.sum()
    return (ds * weights).mean(dim=["lat", "lon"])


def climatology(ds):
    seasonal_clim = ds.groupby("time.season").mean(dim="time")
    return seasonal_clim


def anomaly(ds):
    seasonal_clim = climatology(ds)
    seasonal_anom = ds.groupby("time.season") - seasonal_clim
    return seasonal_anom
