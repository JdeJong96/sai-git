#!/usr/bin/env python3
# *_* coding: utf-8 *_*

import xarray as xr


def wmean(ds:[xr.Dataset,xr.DataArray], w:xr.DataArray, dims, **kwargs):
    """wrapper for xarray weighted mean
    
    Input:
    ds : data to average
    w : weights
    dims : iterable of dimensions to average over
    kwargs : keyword arguments passed on to .mean(), e.g. keep_attrs
    
    Manual fixes:
        1) only apply averaging along subset of dimensions that is also in 
            dims (returns the unaveraged data if no overlapping dimensions)
        2) always copy coordinate attributes to result
        3) remove weighted operator if w.dims is no subset of ds.dims to 
            prevent broadcasting
        4) let function wmean determine keep_attrs per dataarray instead of 
            map() which applies one value to the whole dataset
    """
    if isinstance(ds, xr.Dataset):
        global WMEAN_ATTRS
        WMEAN_ATTRS = {}
        dsm = ds.map(wmean, False, [w, dims])
        if kwargs.get('keep_attrs', False):
            for v in dsm.data_vars:
                dsm[v].attrs = WMEAN_ATTRS[v] # 4
        return dsm
    if 'WMEAN_ATTRS' in globals():
        WMEAN_ATTRS[ds.name] = ds.attrs
    #avgdims = tuple(set(ds.dims).intersection(dims)) # 1
    coordattrs = {c:ds[c].attrs for c in ds.coords}
    avgdims = [dim for dim in dims if (dim in ds.dims) and not (dim in w.dims)]
    for dim in avgdims:
        ds = ds.mean(avgdims, **kwargs)
    avgdims = [dim for dim in dims if (dim in ds.dims) and (dim in w.dims)]
    for dim in avgdims:
        ds = ds.weighted(w).mean(avgdims, **kwargs)
    # if len(avgdims)==0: # 1
    #     print(f'{dims=} not in {ds.dims=}')
    #     print(set(ds.dims),dims,set(ds.dims).intersection(dims))
    #     return ds
    # coordattrs = {c:ds[c].attrs for c in ds.coords}
    # if set(w.dims).issubset(ds.dims):
    #     print('taking weighted mean')
    #     dsm = ds.weighted(w).mean(avgdims, **kwargs)
    # else: # 3
    #     print('taking unweighted mean')
    #     dsm = ds.mean(avgdims, **kwargs)
    for c in ds.coords:
        ds[c].attrs.update(coordattrs[c]) # 2
    return ds