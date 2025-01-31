import time
import sys
import xarray as xr
import numpy as np

Rearth = 6371000
lon_edges = xr.DataArray(np.linspace(-180,180,1441), coords=None, dims='xe')
lat_edges = xr.DataArray(np.linspace(-90,90,721), coords=None, dims='ye')
lons = lon_edges.rolling(xe=2,center=True).mean().dropna('xe').swap_dims(xe='x')
lats = lat_edges.rolling(ye=2,center=True).mean().dropna('ye').swap_dims(ye='y')
lats,lons = xr.broadcast(lats,lons)
dlons = np.deg2rad(lon_edges).diff('xe').swap_dims(xe='x')
dsinlats = np.sin(np.deg2rad(lat_edges)).diff('ye').swap_dims(ye='y')
area = xr.DataArray((Rearth**2 * dsinlats * dlons).data, coords={'lat':lats,'lon':lons}, dims=('y','x'))
ds = xr.Dataset({'area':area},coords={'lev':np.linspace(1000,100,32), 'time':np.arange(2000,2002,1/12)})
ds['T'] = (286 + 10*np.cos(2*np.deg2rad(ds.lat)) + 10*np.log(ds.lev/ds.lev[0]) + (ds.time-2000)) 
gw = ds.area/ds.area.mean()

def gmean(obj):
    def avg(obj, axis=None, weights=None):
        print(obj.shape, weights.shape)
        return np.average(obj, axis=axis, weights=weights)
    return xr.apply_ufunc(avg,obj,input_core_dims=[['y','x']],kwargs={'axis':(-2,-1),'weights':obj.area})

def gmeanall(obj):
    return xr.apply_ufunc(np.average,obj,input_core_dims=[['y','x']],dask='allowed',kwargs={'axis':(-2,-1),'weights':obj.area})

def gmeanpar(obj):
    return xr.apply_ufunc(np.average,obj,input_core_dims=[['y','x']],dask='parallelized',kwargs={'axis':(-2,-1),'weights':obj.area})

#ds = ds.chunk({'lev':1,'time':1})
#print(f"Chunking dataset...\n{ds.chunks}")
#sys.exit(0)

_ = gmean(ds)
_ = gmeanall(ds)
_ = gmeanpar(ds)

start = time.perf_counter()
_ = gmean(ds).T.compute()
end = time.perf_counter()
print(f"gmean: {end-start:.6f} seconds")

start = time.perf_counter()
_ = gmeanall(ds).T.compute()
end = time.perf_counter()
print(f"gmeanall: {end-start:.6f} seconds")

start = time.perf_counter()
_ = gmeanpar(ds).T.compute()
end = time.perf_counter()
print(f"gmeanpar: {end-start:.6f} seconds")

ds = ds.chunk({'lev':1,'time':1})
print(f"Chunking dataset...\n{ds.chunks}")

start = time.perf_counter()
_ = gmean(ds).T.compute()
end = time.perf_counter()
print(f"gmean: {end-start:.6f} seconds")

start = time.perf_counter()
_ = gmeanall(ds).T.compute()
end = time.perf_counter()
print(f"gmeanall: {end-start:.6f} seconds")

start = time.perf_counter()
_ = gmeanpar(ds).T.compute()
end = time.perf_counter()
print(f"gmeanpar: {end-start:.6f} seconds")
