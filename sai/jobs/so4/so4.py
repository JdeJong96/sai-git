#!/usr/bin/env python3
# *_* coding: utf-8 *_*


def main():
    # read script arguments 
    parser = argparse.ArgumentParser(
        description='Calculate temperature gradients T0, T1 and T2 time series as in GLENS')
    parser.add_argument('infiles', nargs='+', help='input netCDF files')
    parser.add_argument('outfile', help='output netCDF file')
    args = parser.parse_args()
    if os.path.exists(args.outfile):
        raise ValueError(f'output file {args.outfile} already exists.')
    
    # open dataset and set time to center of time_bnds
    ds = xr.open_mfdataset(args.infiles, data_vars="minimal", coords="minimal", 
    join="exact", compat="override", chunks=None)
    print(f"+{perf_counter()-t0:.1f} sec: opened dataset")
    time = ('time', ds.time_bnds.mean('nbnd').data, ds.time.attrs)
    ds = ds.assign_coords({'ctime':time}).swap_dims({'time':'ctime'})
    ds = ds.drop_vars('time').rename({'ctime':'time'})

    # calculate T0, T1 and T2
    sinlat = np.sin(np.deg2rad(ds.lat))
    gmean = lambda da: da.weighted(ds.gw).mean(('lat','lon'), keep_attrs=True)
    ds['P'] = ds.hyam*ds.P0 + ds.hybm*ds.PS
    ds = ds[['so4_a1','so4_a2','so4_a3','P']]
    ds = ds.sel(time=slice('2068','2098')).mean(('lon','time'))
    ds.to_netcdf(args.outfile)
    print(f"+{perf_counter()-t0:.1f} sec: created {args.outfile}")
    

if __name__ == '__main__':
    import os
    import sys
    import argparse
    from time import perf_counter
    import numpy as np
    import xarray as xr
    import dask
    from dask.distributed import Client
    t0 = perf_counter()
    #client = Client() # for parallel opening
    #print(f"+{perf_counter()-t0:.1f} sec: {client}")
    main()
