import os
import sys
import glob
import xarray as xr

files = sorted(sys.argv[1:-1])
outfile = sys.argv[-1]

xr.backends.list_engines()
xr.show_versions()

with xr.open_mfdataset(files, chunks={'time':-1,'lev':1,'lon':'auto','lat':'auto'}, parallel=True) as ds:
    print(ds.chunks, flush=True)
    ds.mean('time').to_netcdf(outfile)

print(f"created {outfile}")
