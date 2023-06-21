import os
import sys
import argparse
import logging
from numba import guvectorize
import numpy as np
import xarray as xr

PLEVS = (250, 850)  # pressure levels (hPa) to interpolate to
PRES = 'P'          # short name of 3D atmospheric pressure


def parse_cla():
    """parse command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+', help='climate data files')
    parser.add_argument('outfile', help='output file')
    parser.add_argument('-v', '--verbose', help='modify output verbosity', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, 
                        format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%d-%m-%Y %H:%M:%S")
    logging.info(f"data files in {os.path.dirname(args.files[0])}:" \
                 f"\n{os.linesep.join([os.path.basename(f) for f in args.files])}")
    return args


def xr_open_dataset(files):
    """create Dataset, chunking over dimension ncol"""
    if ('ne30_g16' in files[0]) or ('ne30g16' in files[0]):
        nchunk = 6944
    elif ('ne120_t12' in files[0]) or ('ne120t12' in files[0]):
        nchunk = 10000
    else:
        raise NotImplementedError(f'expected spectral grid')
    return xr.open_mfdataset(files, chunks={'ncol':nchunk})


def get_vertical_dimension(ds):
    """check for known vertical dimensions"""
    VDIMS = ['lev','plev','z','hybrid']  # dimension names to search for
    dsdims = list(ds.dims)
    for vdim in VDIMS:
        if vdim in dsdims:
            break
    else:
        msg = f'Could not find vertical dimension in Dataset with dimensions {dsdims}.'
        raise NotImplementedError(msg)
    return vdim


def hybrid_to_pressure(ds):
    """Calculate air pressure from hybrid coefficients"""
    if PRES in ds:
        return ds[PRES]
    vdim = get_vertical_dimension(ds)
    if vdim not in ['lev','hybrid']:
        raise NotImplementedError(f'Vertical coordinate ({vdim}) must be hybrid')
    P = ds.hyam * ds.P0 + ds.hybm * ds.PS
    P.attrs = {'standard_name':'air_pressure','long_name':'air pressure'}
    if hasattr(ds.PS, 'units'):
        P.attrs.update({'units':ds.PS.units})
    else:
        logging.warning("assuming PS has units Pa")
        P.attrs.update({'units':'Pa'})
    if hasattr(ds.PS, 'cell_methods'):
        P.attrs.update({'cell_methods':ds.PS.cell_methods})
    return P


@guvectorize(
    "(float64[:], float64[:], float64[:], float32[:])",
    " (n), (n), (m) -> (m)",
    nopython=True,
)
def interp1d_gu(f, x, xi, out):
    """Interpolate field f(x) to xi in ln(x) coordinates."""
    i, imax, x0, f0 = 0, len(xi), x[0], f[0]
    while xi[i]<x0 and i < imax:
        out[i] = np.nan      
        i = i + 1 
    for x1,f1 in zip(x[1:], f[1:]):
        while xi[i] <= x1 and i < imax:
            out[i] = (f1-f0)/np.log(x1/x0)*np.log(xi[i]/x0)+f0
            i = i + 1
        x0, f0 = x1, f1
    while i < imax:
        out[i] = np.nan
        i = i + 1


def interpolate_to_pressure(ds, names=['U','V'], vdim=None):
    names_valid = [name for name in names if name in ds]
    if names_valid is not names:
        logging.warning(f"variables {set(names)-set(names_valid)} not in Dataset containing: {list(ds.variables)}")
    logging.info(f"variables to interpolate: {names_valid}")
    if any([p>1100 for p in PLEVS]):
        logging.warning(f"pressure levels must be in hPa (PLEVS = {PLEVS})")
    if vdim is None:
        vdim = get_vertical_dimension(ds)
    ds = ds.assign_coords({'p':(       # create new pressure coordinate 'p'
        'plev', 
        [p*100 for p in sorted(PLEVS, reverse=True)], 
        {'standard_name':'air_pressure','long_name':'air pressure','units':'Pa'}
    )})
    return xr.apply_ufunc(
        interp1d_gu,  ds[names_valid], ds[PRES], ds.p,
        input_core_dims=[[vdim], [vdim], [*ds.p.dims]], 
        output_core_dims=[[*ds.p.dims]], 
        exclude_dims=set((vdim,)),  
        output_dtypes=[ds[name].dtype for name in names_valid],
        keep_attrs=True,
    ).transpose(*ds.p.dims,...).compute()
        

def main():
    args = parse_cla()
    ds = xr_open_dataset(args.files)
    logging.info(f"Dataset chunks: {ds.chunks}")
    vdim = get_vertical_dimension(ds)
    if PRES not in ds:
        ds[PRES] = hybrid_to_pressure(ds)
    dsi = interpolate_to_pressure(ds)
    ds.close()
    dsi.to_netcdf(args.outfile)
    return
    

if __name__ == '__main__':
    main()
