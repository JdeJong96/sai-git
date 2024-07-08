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
    T0 = gmean(ds.TREFHT).rename('T0')
    T1 = gmean(ds.TREFHT*sinlat).rename('T1')
    T1.attrs.update({'long_name':'Interhemispheric temperature gradient'})
    T2 = gmean(ds.TREFHT*(3*sinlat**2-1)/2).rename('T2')
    T2.attrs.update({'long_name':'Equator-pole temperature gradient'})
    dsm = xr.merge((T0,T1,T2,ds.time_bnds))
    dsm.time.attrs.update(ds.time.attrs) 

    # take annual mean
    mean_bnds = lambda da: xr.DataArray([da.values.min(), da.values.max()], 
        {'time':da.time.mean('time', keep_attrs=True)}, 'nbnd')
    dsy = dsm.groupby('time.year').mean('time', keep_attrs=True)
    dsy.year.attrs = {
        'long_name':'time', 'units':'simulated year', 'calendar':'noleap'
    }
    dsy = dsy.assign_coords(time = ('year',dsm.time.groupby('time.year').mean('time').data, dsm.time.attrs))
    new_bounds = dsm.time_bnds.groupby('time.year').map(mean_bnds)
    dsy['time_bnds'] = (new_bounds.dims, new_bounds.data, dsm.time_bnds.attrs)
    dsm.close()
    dsy = dsy.swap_dims({'year':'time'})
    dsy.time.encoding['units'] = 'days since 0001-01-01'

    # write output
    dsy.attrs = {'history':f'python temperaturegradients.py {args.infiles} {args.outfile}'}
    dsy.to_netcdf(args.outfile)
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
    client = Client() # for parallel opening
    print(f"+{perf_counter()-t0:.1f} sec: {client}")
    main()
