#!/usr/bin/env python3
# *_* coding: utf-8 *_*

"""Calculate temperature gradients T0, T1 and T2 time series as in the GLENS paper"""

infiles = '/Users/jasperdejong/Documents/PhD/Geo/sai-git/sai/data/deg5/mres_b.e10.B2000_CAM5.5deg.001.cam2.h0.????-??.nc'
outfile = 'mres.sai2050.tempgradients.nc'

def main():
    # open dataset and set time to center of time_bnds
    ds = xr.open_mfdataset(infiles, data_vars="minimal", coords="minimal", 
                           join="exact", compat="override", chunks=None)
    time = ('time', ds.time_bnds.mean('nbnd').data, ds.time.attrs)
    ds = ds.assign_coords({'ctime':time}).swap_dims({'time':'ctime'})
    ds = ds.drop_vars('time').rename({'ctime':'time'})

    # calculate T0, T1 and T2 and write file
    sinlat = np.sin(np.deg2rad(ds.lat))
    gmean = lambda da: da.weighted(ds.gw).mean(('lat','lon'), keep_attrs=True)
    T0 = gmean(ds.TREFHT).rename('T0')
    T1 = gmean(ds.TREFHT*sinlat).rename('T1')
    T2 = gmean(ds.TREFHT*(3*sinlat**2-1)/2).rename('T2')
    xr.merge((T0,T1,T2)).groupby('time.year').mean('time').to_netcdf(outfile)
    

if __name__ == '__main__':
    import numpy as np
    import xarray as xr
    from dask.distributed import Client
    client = Client() # set up local cluster
    main()