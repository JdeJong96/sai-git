import os
import sys
from time import perf_counter
import argparse
import logging
#print(f'Python version: {sys.version}')
#print(f'Path: {sys.path}')

from numba import guvectorize
import numpy as np
from dask.distributed import Client
import xarray as xr
xr.set_options(keep_attrs=True)
#xr.show_versions()

PLEVS = (250, 850)  # pressure levels (hPa) for shear calculation

# variable names (change values if needed)
# VARS = {'U':'U','V':'V','P':'P','gw':'gw', 'time_bnds':'time_bnds','lsm':'lsm'} # pressure levels
VARS = {'U':'U','V':'V','hyam':'hyam', 'hybm':'hybm','P0':'P0','PS':'PS',
        'gw':'gw', 'time_bnds':'time_bnds','lsm':'LANDFRAC'} # hybrid model levels

LABEL = 'upper' # ['upper','lower'], level where VSHEAR is defined
VDIM = 'lev'  # vertical dimension (e.g. 'lev','plev','z','hybrid')
CHUNKS = {'time':1,'ncol':'auto', VDIM:-1} # array chunk size for parallel computation
NEWPRES = xr.DataArray(
    data=100*np.array(sorted(PLEVS), dtype='float64'),
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
    while pi[i]<p0 and i < imax:
        out[i] = np.nan      
        i = i + 1 
    for p1,f1 in zip(p[1:], f[1:]):
        while pi[i] <= p1 and i < imax:
            out[i] = (f1-f0)/np.log(p1/p0)*np.log(pi[i]/p0)+f0
            i = i + 1
        p0, f0 = p1, f1
    while i < imax:
        out[i] = np.nan
        i = i + 1


def xr_interpolate_pressure(ds):
    """wrapper for logpressure_interp1d_gu"""
    # calculate pressure if needed and interpolate
    if 'P' not in VARS:
        logging.info(f"calculating 'P' from hybrid parameters")
        pres = ds[VARS['hyam']] * ds[VARS['P0']] + ds[VARS['hybm']] * ds[VARS['PS']]
    else:
        pres = ds[VARS['P']]
    ds = ds.assign_coords({NEWPRES.name:NEWPRES})
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
    return ds
    

def check_globals(ds):
    """Check global variables defined in this file
    
    When faulty, quits program with helpful error messages
    """
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
    if ('P' not in VARS):
        missing_terms = [VARS[t] for t in ['hyam','hybm','P0','PS'] if VARS[t] not in ds]
        if any(missing_terms):
            logging.error(f"{missing_terms} not in dataset")
            fatal = True
        else:
            for v in ['PS','P0']:
                var = ds[VARS[v]]
                if not hasattr(var, 'units'):
                    logging.warning(f"assuming {var.name} has units 'Pa'")
                elif var.units in ['Pa','pa']:
                    logging.info(f"{var.name} units: {var.units}")
                else:
                    logging.error(f"{var.name} units: {var.units}, expected Pa")
                    fatal = True
            logging.info(f"using {[VARS[v] for v in ['hyam','hybm','P0','PS']]} to calculate pressure")
    else:
        logging.info(f"using pressure '{VARS['P']}'")
    if fatal:
        logging.critical(f"fatal errors occurred for dataset:\n{ds}")
        logging.critical(f"resolve errors first, aborting...")
        sys.exit(1)
    return


def main():
    time0 = perf_counter()  # start timer
    client = Client() # for parallel computing

    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+', help='input file(s)')
    parser.add_argument('outfile', help='output file')
    parser.add_argument('-f', help='used by jupyter')
    parser.add_argument('-v', '--verbose', help='modify output verbosity', 
                        action='store_true')
    args = parser.parse_args()
    if os.path.exists(args.outfile):
        raise ValueError(f'file {args.outfile} already exists')

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
    
    # wind shear calculation
    time1 = perf_counter()  # start timer
    levstr = f'{max(PLEVS)}-{min(PLEVS)} hPa'
    logging.info(f"opening [{args.files[0]} - {args.files[-1]}] with chunks {CHUNKS}")
    with xr.open_mfdataset(args.files, data_vars="minimal", coords="minimal", 
        join="exact", compat="override", chunks=CHUNKS) as ds:
        time2 = perf_counter()
        logging.info(f"...succes! opening took {time2-time1:.2f} seconds")
        time = ('time', ds.time_bnds.mean('nbnd').data, ds.time.attrs)
        ds = ds.assign_coords({'ctime':time}).swap_dims({'time':'ctime'})
        ds = ds.drop_vars('time').rename({'ctime':'time'})
        ds = ds[list(VARS.values())]
        check_globals(ds)
        hasplev = NEWPRES.dims[0] in ds.dims
        if (not hasplev) or (hasplev and (not (ds[NEWPRES.dims[0]] == NEWPRES).all())):
            ds = xr_interpolate_pressure(ds)
        USHEAR = ds[VARS['U']].diff(NEWPRES.dims[0], label=LABEL).squeeze()
        VSHEAR = ds[VARS['V']].diff(NEWPRES.dims[0], label=LABEL).squeeze()
        ds['VWS'] = np.sqrt(USHEAR**2 + VSHEAR**2)
        ds.VWS.attrs.update({
            'long_name':'vertical wind shear '+levstr,
            'standard_name':'wind_speed_shear',
            'units':ds[VARS['U']].units,
        })
        ds.attrs.update({'history':
            f'python windshear.py [{args.files[0]} - {args.files[-1]}] {args.outfile}'})
        ds.time.encoding['units'] = 'days since 0001-01-01'
        ds.VWS.encoding['dtype'] = 'float32'
        ds[['VWS',VARS['gw'],VARS['time_bnds'],VARS['lsm']]].to_netcdf(args.outfile)
        time3 = perf_counter()
        logging.info(f"processed all data in {time3-time1:.2f} seconds")
    return


if __name__ == '__main__':
    main()
