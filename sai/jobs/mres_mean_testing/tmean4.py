import os
import sys
import glob
from dask.distributed import Client, performance_report
import xarray as xr

files = sorted(sys.argv[1:-1])
outfile = sys.argv[-1]

def main():    
    with xr.open_dataset(files[0]) as ds:
        print(ds)
    client = Client()
    print(client)
    with performance_report(filename='dask-report.html'):
        with xr.open_mfdataset(files, chunks='auto', parallel=True, drop_variables=['time_bnds','date_written','time_written']) as ds:
            print(ds.chunks, flush=True)
            ds.mean('time').to_netcdf(outfile)
        print(f"created {outfile}")

if __name__ == "__main__":    
    main()
