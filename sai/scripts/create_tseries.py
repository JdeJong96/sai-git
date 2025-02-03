import os
import glob
import shutil
from functools import partial
from matplotlib.colors import Normalize, CenteredNorm
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
%matplotlib inline
#plt.style.use('seaborn-talk') 
plt.style.use('seaborn-v0_8-talk')
import cmocean
import cftime
import datetime
import cartopy.crs as ccrs
import numpy as np
import dask
from dask.distributed import LocalCluster, Client, progress
import netCDF4 as nc
import xarray as xr

# files for medium resolution (0.5 deg atm, 0.1 deg ocn)

# control (RCP8.5, yearly)
cdir = '/projects/0/prace_imau/prace_2013081679/cesm1_0_4/f05_t12/rcp8.5_co2_f05_t12/'
camc = cdir+'atm/hist/yearly/rcp8.5_co2_f05_t12.cam2.h0.avg????.nc' # correct time stamps

# spinup (monthly)
sdir = '/projects/0/nwo2021025/cesm1_0_4/outputdata/spinup_pd_maxcores_f05_t12/output/'
cams = cdir+'atm/hist/spinup_pd_maxcores_f05_t12.cam2.h0.????-??.nc' # add 1800 years
ices = cdir+'ice/hist/spinup_pd_maxcores_f05_t12.001.cice.h.????-??.nc'
clms = cdir+'lnd/hist/spinup_pd_maxcores_f05_t12.clm2.h0.????-??.nc'
pops = cdir+'ocn/hist/spinup_pd_maxcores_f05_t12.pop.h.????-??.nc'

# geo (monthly)
gdir = '/projects/0/nwo2021025/archive/mres_b.e10.B2000_CAM5.f05_t12.001/'
camg = gdir+'atm/hist/mres_b.e10.B2000_CAM5.f05_t12.001.cam2.h0.????-??.nc'
iceg = gdir+'ice/hist/mres_b.e10.B2000_CAM5.f05_t12.001.cice.h.????-??.nc'
clmg = gdir+'lnd/hist/mres_b.e10.B2000_CAM5.f05_t12.001.clm2.h0.????-??.nc'
popg = gdir+'ocn/hist/mres_b.e10.B2000_CAM5.f05_t12.001.pop.h.????-??.nc'
aerg = gdir+'strataero/ozone_strataero_1999-2100_SSP585_CAMfeedback.nc'
volg = gdir+'volcaero/volcaero_1999-2100_SSP585_CAMfeedback.nc'

hrgdir = lambda i: f'/projects/0/nwo2021025/archive/hres_b.e10.B2000_CAM5.f02_t12.started_2092-12.00{i}/'
camhrg = lambda i: hrgdir(i) + f'atm/hist/hres_b.e10.B2000_CAM5.f02_t12.started_2092-12.00{i}.cam2.h0.????-??.nc'

hrgfdir = lambda i: f'/projects/0/nwo2021025/archive/hres_b.e10.B2000_CAM5.f02_t12.started_2092-12.00{i}_FAIL/'
camhrgf = lambda i: hrgfdir(i) + f'atm/hist/hres_b.e10.B2000_CAM5.f02_t12.started_2092-12.00{i}.cam2.h0.????-??.nc'

hrsfdir = '/projects/0/nwo2021025/archive/hres_b.e10.B2000_CAM5.f02_t12.spinup_from_mres_2092-01_FAIL/'
camhrsf = hrsfdir+'atm/hist/hres_b.e10.B2000_CAM5.f02_t12.spinup_from_mres_2092-01.cam2.h0.2092-??.nc'
volhrsf = hrsfdir+'volcaero/volcaero_1999-2100_SSP585_CAMfeedback.nc'

hrsdir = '/projects/0/nwo2021025/archive/hres_b.e10.B2000_CAM5.f02_t12.spinup_from_mres_2092-01/'
camhrs = hrsdir+'atm/hist/hres_b.e10.B2000_CAM5.f02_t12.spinup_from_mres_2092-01.cam2.h0.2092-??.nc'
volhrs = hrsdir+'volcaero/volcaero_1999-2100_SSP585_CAMfeedback.nc'

# initiate dask cluster for parallel computing
cluster = LocalCluster(n_workers=16, threads_per_worker=1, memory_limit="14GiB")
client = Client(cluster)
cluster, client

def center_time(ds):
    """set time stamps to center of time_bnds"""
    time = ('time', ds.time_bnds.mean('nbnd').data, ds.time.attrs)
    ds = ds.assign_coords({'ctime':time}).swap_dims({'time':'ctime'})
    return ds.drop_vars('time').rename({'ctime':'time'})


def shift_time(ds, ndays):
    """shift time of dataset by ndays days"""
    dt = datetime.timedelta(days=ndays)
    t0, t1 = ds.time.data[[0,-1]]
    print(f"Shifting time period from {t0}--{t1} to {t0+dt}--{t1+dt}")
    ds = ds.assign(time_bnds=(ds.time_bnds.dims,(ds.time_bnds.compute()+dt).data,ds.time_bnds.attrs))
    return ds.assign(time=(ds.time.dims,(ds.time+dt).data,ds.time.attrs))


def wmean(ds:[xr.Dataset,xr.DataArray], w:xr.DataArray, dims, **kwargs):
    """wrapper for xarray weighted mean
    
    Input:
    ds : data to average
    w : weights
    dims : iterable of dimensions to average over
    kwargs : keyword arguments passed on to .mean(), e.g. keep_attrs
    
    Manual fixes:
        1) only apply averaging along subset of dimensions that is also in 
            dims (returns the unaveraged data if no overlapping dimensions)
        2) always copy coordinate attributes to result
        3) remove weighted operator if w.dims is no subset of ds.dims to 
            prevent broadcasting
        4) let function wmean determine keep_attrs per dataarray instead of 
            map() which applies one value to the whole dataset
    """
    if isinstance(ds, xr.Dataset):
        global WMEAN_ATTRS
        WMEAN_ATTRS = {}
        dsm = ds.map(wmean, False, [w, dims])
        if kwargs.get('keep_attrs', False):
            for v in dsm.data_vars:
                dsm[v].attrs = WMEAN_ATTRS[v] # 4
        return dsm
    if 'WMEAN_ATTRS' in globals():
        WMEAN_ATTRS[ds.name] = ds.attrs
    avgdims = tuple(set(ds.dims).intersection(dims)) # 1
    if len(avgdims)==0: # 1
        return ds
    coordattrs = {c:ds[c].attrs for c in ds.coords}
    if set(w.dims).issubset(ds.dims):
        dsm = ds.weighted(w).mean(avgdims, **kwargs)
    else: # 3
        dsm = ds.mean(avgdims, **kwargs)
    for c in dsm.coords:
        dsm[c].attrs.update(coordattrs[c]) # 2
    return dsm

open_kwargs = {'data_vars': 'minimal', 'coords': 'minimal', 'compat': 'override', 'parallel': True, 'decode_times':False}
cam = {
    # 'cnt': shift_time(center_time(xr.open_mfdataset(camc, data_vars='minimal', coords='minimal', compat='override')), ndays=365*1800),
    'cnt': center_time(xr.open_mfdataset(camc, **open_kwargs)),
    'geo': center_time(xr.open_mfdataset(camg, **open_kwargs)),
    # 'geof': center_time(xr.open_mfdataset(, **open_kwargs)),
    'hr1': center_time(xr.open_mfdataset(camhrg(1), **open_kwargs)),
    #'hr1f': center_time(xr.open_mfdataset(camhrgf(1), **open_kwargs)),
    'hr2': center_time(xr.open_mfdataset(camhrg(2), **open_kwargs)),
    #'hr3': center_time(xr.open_mfdataset(camhrg(3), **open_kwargs)),
    #'hr4': center_time(xr.open_mfdataset(camhrg(4), **open_kwargs)),
    #'hr5': center_time(xr.open_mfdataset(camhrg(5), **open_kwargs)),
    'hrs': center_time(xr.open_mfdataset(camhrs, **open_kwargs)),
    #'hrsf': center_time(xr.open_mfdataset(camhrsf, **open_kwargs))
}

camm = {k:wmean(wmean(v, v.gw, ('lat','lon'), keep_attrs=True), v.w_stag, ('slat', 'slon'), keep_attrs=True) for k,v in cam.items()}

for sim in camm:
    print(sim)
    camm[sim].to_netcdf(f'/home/jasperdj/copy240826/{sim}.nc')