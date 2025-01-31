#!/usr/bin/env python3
# *_* coding: utf-8 *_*

regions = {
    'NA':{'lat': slice(5,20), 'lon': slice(275,345)}, # (15-85 E) TC season: June-November
    'WNP':{'lat': slice(5,20), 'lon': slice(110,180)}, # ~ May-November
    'TROP': {'lat': slice(-30,30)}
}
reglabels = {K:' '.join([f"{v.start,v.stop}{'N' if k=='lat' else 'E'}" for (k,v) in V.items()]) for (K,V) in regions.items()}
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

    # calculate MDR means and tropical mean
    gmean = lambda da: da.weighted(ds.gw).mean(('lat','lon'), keep_attrs=True)
    ds['TNA'] = gmean(ds.TREFHT.sel(regions['NA']))
    ds['TWP'] = gmean(ds.TREFHT.sel(regions['WNP']))
    ds['TTROP'] = gmean(ds.TREFHT.sel(regions['TROP']))
    ds['PLNA'] = gmean(ds.PRECL.sel(regions['NA']))
    ds['PCNA'] = gmean(ds.PRECC.sel(regions['NA']))
    ds['PLWP'] = gmean(ds.PRECL.sel(regions['WNP']))
    ds['PCWP'] = gmean(ds.PRECC.sel(regions['WNP']))
    ds.TNA.attrs.update({'long_name':f'North Atlantic surface temperature {reglabels["NA"]}'})
    ds.TWP.attrs.update({'long_name':f'Western North Pacific surface temperature {reglabels["WNP"]}'})
    ds.TTROP.attrs.update({'long_name':f'Tropical surface temperature {reglabels["TROP"]}'})
    ds.PLNA.attrs.update({'long_name':f'North Atlantic large scale precipitation {reglabels["NA"]}'})
    ds.PCNA.attrs.update({'long_name':f'North Atlantic convective precipitation {reglabels["NA"]}'})
    ds.PLWP.attrs.update({'long_name':f'Western North Pacific large scale precipitation {reglabels["WNP"]}'})
    ds.PCWP.attrs.update({'long_name':f'Western North Pacific convective precipitation {reglabels["WNP"]}'})
    
    # calculate relative T maps
    ds = ds[['OCNFRAC','gw','time_bnds','TREFHT','PRECL','PRECC','TNA','TWP','TTROP','PLNA','PCNA','PLWP','PCWP']].sel(regions['TROP'])
    
    # write output
    ds.attrs.update({
        'description':'Surface temperatures in different main development regions and the tropics',
        'history':f'python MDR_relative_temp_pcip.py [{args.infiles[0]} - {args.infiles[-1]}] {args.outfile}'
    })
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
    client = Client() # for parallel opening
    print(f"+{perf_counter()-t0:.1f} sec: {client}")
    main()
