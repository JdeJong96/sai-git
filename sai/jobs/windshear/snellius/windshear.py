#!/usr/bin/env python3
# *_* coding: utf-8 *_*

"""Read CESM output data and calculate vertical wind shear

Run with >> python windshear.py [-v,--verbose] *files outfile

This script reads CESM output data, interpolates variables NAMES to
pressure levels PLEVS. The wind shear is then calculated as the 
difference between the horizontal winds at these levels. 

The interpolation is performed linearly in ln(P) coordinates by applying
logpressure_interp1d_gu() over every 1D vertical column in the data. If 
the  pressure P is not present in the data, P will be calculated using
FORMULA_TERMS, a mapping to relevant dataset variables to convert
hybrid coordinates to pressure. 

Parallel computing is supported by mapping the 1D vertical interpolation
function to different chunks of the data arrays, controllable by the
CHUNKS parameter. Mind that the vertical dimension is not allowed to be 
chunked because the gufunc operates along this dimension (leave VDIM:-1)

The vertical wind shear and other variables are stored in outfile.
"""

import os
import sys
import time
import argparse
import logging
print(f'Python version: {sys.version}')
print(f'Path: {sys.path}')

from numba import guvectorize
import numpy as np
import xarray as xr
xr.set_options(keep_attrs=True)
#xr.show_versions()

# define constants
YEAR_RANGE = range(2070,2073) # years to analyze
PLEVS = (250, 850)  # pressure levels (hPa) for shear calculation
PRES = 'P'    # air pressure 
HYAM = 'hyam' # hybrid a coefficient (only used if P not present)
HYBM = 'hybm' # hybrid b coefficient (see HYAM)
PS = 'PS'     # surface pressure (see HYAM)
P0 = 'P0'     # reference pressure hybrid coordinate (see HYAM)
NAMES = []   # list of variables to interpolate/store
U, V = 'U', 'V' # zonal and meridional wind
LABEL = 'upper' # ['upper','lower'], level where VSHEAR is defined
VDIM = 'lev'  # vertical dimension (e.g. 'lev','plev','z','hybrid')
CHUNKS = {'time':1,'ncol':'auto', VDIM:-1} # array chunk size for parallel computation
NEWPRES = xr.DataArray(
    data=100*np.array(sorted(PLEVS, reverse=True), dtype='float64'),
    dims='plev',
    name='p',
    attrs={'standard_name':'air_pressure',
           'long_name':'air pressure',
           'units':'Pa'}
)


@guvectorize([
    "(float64[:], float64[:], float64[:], float64[:])",
    "(float32[:], float32[:], float64[:], float32[:])"
], " (n), (n), (m) -> (m)", nopython=True, cache=True)
def logpressure_interp1d_gu(f, p, pi, out):
    """interpolate field f(p) to pi in ln(p) coordinates"""
    i, imax, p0, f0 = 0, len(pi), p[0], f[0]
    while pi[i]>p0 and i < imax:
        out[i] = np.nan      
        i = i + 1 
    for p1,f1 in zip(p[1:], f[1:]):
        while pi[i] >= p1 and i < imax:
            out[i] = (f1-f0)/np.log(p1/p0)*np.log(pi[i]/p0)+f0
            i = i + 1
        p0, f0 = p1, f1
    while i < imax:
        out[i] = np.nan
        i = i + 1


def xr_interpolate_pressure(ds):
    """wrapper for logpressure_interp1d_gu"""
    # calculate pressure if needed and interpolate
    if PRES not in ds:
        logging.info(f"calculating {PRES} from hybrid parameters")
        pres = ds[HYAM] * ds[P0] + ds[HYBM] * ds[PS]   
    else:
        pres = ds[PRES]
    ds = ds[NAMES].assign_coords({NEWPRES.name:NEWPRES})
    logging.info(f"new coordinate: {ds[NEWPRES.name].coords}")
    logging.info("starting interpolation...")
    vdimvars = [v for v in ds.data_vars if hasattr(ds[v], VDIM)]
    if len(vdimvars) == 1:
        vdimvars = vdimvars[0]
    ds[vdimvars] = xr.apply_ufunc(
        logpressure_interp1d_gu,  ds[vdimvars], pres, NEWPRES,
        input_core_dims=[[VDIM], [VDIM], [*NEWPRES.dims]], 
        output_core_dims=[[*NEWPRES.dims]], 
        exclude_dims=set((VDIM,)),
        dask="parallelized",
        keep_attrs=True,
    ).transpose(*NEWPRES.dims,...)
    

def wind_shear(ds):
    """Calculate windshear"""
    USHEAR = ds[U].diff(NEWPRES.name, label=LABEL)
    VSHEAR = ds[V].diff(NEWPRES.name, label=LABEL)
    VWS = np.sqrt(USHEAR**2 + VSHEAR**2)
    levstr = f'{max(PLEVS)}-{min(PLEVS)} hPa'
    USHEAR.attrs.update({
        'long_name':'eastward wind shear'+levstr,
        'standard_name':'eastward_wind_shear',
        'units':ds[U].units,
    })
    VSHEAR.attrs.update({
        'long_name':'northward wind shear'+levstr,
        'standard_name':'northward_wind_shear',
        'units':ds[V].units,
    })
    VWS.attrs.update({
        'long_name':'wind speed shear'+levstr,
        'standard_name':'wind_speed_shear',
        'units':ds[U].units,
    })
    ds['Ushear'] = USHEAR
    ds['Vshear'] = VSHEAR
    ds['VWS'] = VWS
    return ds


def check_globals(ds):
    """Check global variables defined in this file
    
    When faulty, quits program with helpful error messages
    """
    global NAMES
    fatal = False
    if VDIM not in ds:
        logging.error(f"vertical dimension {VDIM} not in dataset")
        fatal = True
    else:
        logging.info(f"vertical dimension: {VDIM}")
    if any(p > 1100 for p in PLEVS):
        logging.error(f"PLEVS {PLEVS} should be in hPa")
        fatal = True
    else:
        logging.info(f"new pressure levels: {PLEVS} hPa")
    if (PRES not in ds):
        missing_terms = [t for t in [HYAM,HYBM,P0,PS] if t not in ds]
        if any(missing_terms):
            logging.error(f"{missing_terms} not in dataset")
            fatal = True
        else:
            for term in [PS,P0]:
                var = ds[term]
                if not hasattr(var, 'units'):
                    logging.warning(f"assuming {var.name} has units 'Pa'")
                elif var.units in ['Pa','pa']:
                    logging.info(f"{var.name} units: {var.units}")
                else:
                    logging.error(f"{var.name} units: {var.units}, expected Pa")
                    fatal = True
            logging.info(f"using {HYAM,HYBM,P0,PS} to calculate '{PRES}'")
    else:
        logging.info(f"using pressure '{PRES}'")
    names = NAMES.copy() if len(NAMES) > 0 else list(ds.data_vars)
    validnames = []
    for name in names:
        if name not in ds:
            logging.warning(f"variable {name} not in dataset")
        else:
            validnames.append(name)
    NAMES = validnames
    if len(NAMES) == 0:
        logging.error("no variables found in dataset")
        fatal = True
    else:
        logging.info(f"variables to interpolate: {NAMES}")
    if fatal:
        logging.critical(f"fatal errors occurred for dataset:\n{ds}")
        logging.critical(f"resolve errors first, aborting...")
        sys.exit(1)


def write_file(ds, fname, year):
    """add year to filename and save as netCDF"""
    fnameparts = list(os.path.splitext(fname))
    fnameparts.insert(1,f"{year:04}")
    newfname = ''.join(fnameparts)
    logging.info(f"writing to {os.path.abspath(fname)}")
    ds.to_netcdf(newfname)
    logging.info(f"SUCCES")


def main():
    time0 = time.perf_counter()  # start timer

    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+', help='input file(s)')
    parser.add_argument('outfile', help='output file')
    parser.add_argument('-f', help='used by jupyter')
    parser.add_argument('-v', '--verbose', help='modify output verbosity', 
                        action='store_true')
    args = parser.parse_args()

    # set up logger 
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%d-%m-%Y %H:%M:%S"
    )
    logging.info(
        f"read path: {os.path.dirname(os.path.abspath(args.files[0]))}:"
        "\ninput file(s):"
        f"\n{os.linesep.join([os.path.basename(f) for f in args.files])}"
    )
    
    # 
    for year in YEAR_RANGE:
        time1 = time.perf_counter()  # start timer
        files_year = [f for f in args.files 
                      if int(f.split('.')[-2][:4]) == year]
        logging.info(f"opening {files_year} with chunks {CHUNKS}")
        logging.info(
            "opening:"
            f"\n{os.linesep.join([os.path.basename(f) for f in files_year])}"
            f"with chunks {CHUNKS}"
        )
        try:
            datasets = [xr.open_dataset(f, chunks=CHUNKS) for f in files_year]
        except Exception:
            [ds.close() for ds in datasets]
            raise
        with xr.merge(datasets, join='exact', 
                      combine_attrs='no_conflicts') as ds:
            logging.info(f"...succes!")
            check_globals(ds)
            ds = wind_shear(ds) # calculate wind shear
            write_file(ds, args.outfile, year)
            time2 = time.perf_counter()
            logging.info(f"processed year {year} in {time2-time1:.2f} seconds")
    logging.info(f"total script time: {time2-time0:.2f} seconds")


if __name__ == '__main__':
    main()
