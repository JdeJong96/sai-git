import numpy as np
import xarray as xr
import matplotlib.pyplot as plt

dpath = "/data2/imau/users/jasper/ihesp"
ds_hrmip_hist = xr.opendataset(dpath+"/hrmip/rcp8.5/tas_Amon_CESM1-CAM5-SE-HR*.nc")
ds_hrmip_hist['tas'].rename('TREFHT')
print(list(ds_hrmip_hist.variables))
ds_hrmip_ext = xr.open_dataset(dpath+"/rcp8.5/hrmip/*.TREFHT.*.nc")

ds_hrmip = xr.open_mfdataset([
    "/hist/hr/mon/TREFHT/*.TREFHT.*.nc",
    "/rcp8.5/hr/*.TREFHT.*.nc"
])
plt.figure(figsize=(12,8))
for loc,label in zip(sublocs,["hrmip","1850control","hrmip","transient"]):
    print(loc, end="  -  ")
    with xr.open_mfdataset(dpath+loc) as ds:
        otime = np.array([t.item().toordinal() for t in ds.time])
        otime = (otime-otime[0])/365
        gw = ds.area / ds.area.sum('ncol')
        tw = ds.TREFHT[:,0] < 1e35
        tw = tw / tw.sum('time')
        #plt.contourf(ds.lon, ds.lat, ds.TREFHT.mean('time'))
        #plt.savefig('TREFmap.png')
        TREF0 = ds.TREFHT.weighted(gw*tw).mean(dim=['time','ncol']).compute()
        Tref = ds.TREFHT.weighted(gw).mean('ncol').data
        Tref[Tref>1e35] = np.nan
        plt.plot(otime, Tref, label=label+f" {TREF0.item():.3f}K")	
        print(f"Mean reference height temperature: {TREF0.item():.3f}K")
plt.title("Monthly mean global mean reference height temperature")
plt.ylabel("Tref (K)")
plt.xlabel("years from 1850 (PI) or 2050 (hrmip/transient)")
plt.legend()
plt.savefig("Tref.png")
