import numpy as np
import xarray as xr

paths = "/data2/imau/users/jasper/ihesp/rcp8.5/ens/mon/*.nc"
print(f"Reading \n {paths}")
ds = xr.open_mfdataset(paths)
print(ds)
print(ds.TREFHT)
print(ds.cosp_sza)
sinLat = np.sin(np.deg2rad(ds.lat))
gw = ds.area / ds.area.sum()
T0 = ds.TREFHT.weighted(gw).mean(dim=['time','ncol']).compute()
print(f"Mean ensemble surface temperature: {T0.item():.3f}K")
