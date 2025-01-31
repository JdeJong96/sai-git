#!/bin/usr/env python3

import sys
import time
import xarray

variable = 'TREFHT'
newvariable = 'TREFHTGA' # global average


def outfile(infiles):
    """Return new name"""
    infiles = sorted(infiles) # sorts by date
    startmonth = ''.join(infiles[0].split('.')[-2].split('-'))
    endmonth = ''.join(infiles[-1].split('.')[-2].split('-'))
    parts = infiles[0].split('.')
    parts.insert(parts.index('h0')+1, newvariable)
    parts[-2] = startmonth+'-'+endmonth
    return '.'.join(parts)


def argparse():
    filename, *args = sys.argv
    if len(args)==0 or any([arg in ['-h','--help'] for arg in args]):
        print('usage: python create_tseries.py [opt] files\n\t-h, --help : show this help')
        sys.exit(0)
    return sorted(args)


def main():
    files = argparse()
    outputfile = outfile(files)
    print(f'creating: {outputfile}')
    with xarray.open_mfdataset(files, parallel=True) as ds:
        gmean = ds[variable].weighted(ds.area).mean('ncol')
        gmean.to_netcdf(outputfile)

if __name__ == '__main__':
    startTime = time.time()
    main()
    print(f"Completed task in {time.time()-startTime:.1f} seconds")
