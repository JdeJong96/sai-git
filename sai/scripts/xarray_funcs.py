#!/usr/bin/env python3
# *_* coding: utf-8 *_*


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
    avgdims = tuple(set(ds.dims).intersection(dims)) # 1
    if len(avgdims)==0: # 1
        return ds
    coordattrs = {c:ds[c].attrs for c in ds.coords}
    if set(w.dims).issubset(ds.dims):
        dsm = ds.weighted(w).mean(avgdims, **kwargs)
    else: # 3
        dsm = ds.mean(avgdims, **kwargs)
    for c in dsm.coords:
        dsm[c].attrs.update(coordattrs[c]) # 2
    return dsm