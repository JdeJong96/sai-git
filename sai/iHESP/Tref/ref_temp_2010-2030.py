import glob
import xarray as xr

GPATH = '/nethome/6272487/sai-git/sai/prep/data/'
DPATH1 = '/data2/imau/users/jasper/ihesp/hrmip/0.25deg/hist/tas/'
DPATH2 = '/data2/imau/users/jasper/ihesp/hrmip/0.25deg/rcp8.5/tas/'

fnameg = GPATH + 'ne120_t12.grid.nc'
fname1 = glob.glob(DPATH1+'*.nc')
fname2 = glob.glob(DPATH2+'*.nc')

with xr.open_dataset(fnameg) as dsg:
    with xr.open_mfdataset(fname1) as ds1:
        with xr.open_mfdataset(fname2) as ds2:
            area = dsg.area
            tas1 = ds1.tas.where(ds1.time.dt.year >= 2010, drop=True)
            tas2 = ds2.tas.where(ds2.time.dt.year < 2030, drop=True)
            print(f'Concatenating tas1 [{tas1.time.min().item().strftime()} - {tas1.time.max().item().strftime()}]'
                  + f' and tas2 [{tas2.time.min().item().strftime()} - {tas2.time.max().item().strftime()}]')
            tas = xr.concat([tas1, tas2], 'time')
            tasmean = tas.weighted(area).mean(('ncol','time')).compute().item()
            print(f'Average [2010-2029] temperature is {tasmean} K')
