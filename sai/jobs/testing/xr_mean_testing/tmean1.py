import os
import sys
import glob
import xarray as xr

files = sorted(sys.argv[1:-1])
outfile = sys.argv[-1]

with xr.open_mfdataset(files) as ds:
    print(ds.chunks, flush=True)
    ds.mean('time').to_netcdf(outfile)

print(f"created {outfile}")
