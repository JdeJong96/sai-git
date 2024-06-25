#!/usr/bin/env python3
# *_* coding: utf-8 *_*

"""Read CESM output data and calculate global mean

Run with >> python globalmean.py [-v,--verbose] *files outfile

The vertical wind shear and other variables are stored in outfile.
"""

import os
import sys
import time
import argparse
import logging
print(f'Python version: {sys.version}')
print(f'Path: {sys.path}')

import numpy as np
import xarray as xr
xr.set_options(keep_attrs=True)


def global_mean(ds):
    """Calculate global mean"""
    gw = ds.gw # grid weights
    # weighted mean does not work smoothly on whole dataset
    # so only apply it to variables with the lat dimension
    ds = ds.drop_vars('lat')
    lvars = [v for v in ds.variables if 'lat' in ds[v].dims]
    ds[lvars] = ds[lvars].weighted(ds.gw).mean('lat')
    # repeat for staggered grid
    ds = ds.drop_vars('slat')
    lvars_w = [v for v in ds.variables if 'slat' in ds[v].dims]
    ds[lvars_w] = ds[lvars_w].weighted(ds.w_stag).mean('slat')
    ds = ds.mean(('lon','slon'))
    return ds


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
    logging.info(f"opening files")
    ds = xr.open_mfdataset(args.files, decode_times=False)
    ds = global_mean(ds)
    logging.info(f"storing interpolated data to {args.outfile}")
    ds.to_netcdf(args.outfile)
    logging.info(f"SUCCES")
    time1 = time.perf_counter()
    logging.info(f"total script time: {time1-time0:.2f} seconds")


if __name__ == '__main__':
    main()
