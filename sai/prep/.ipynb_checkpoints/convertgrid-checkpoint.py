"""convertgrid.py

A set of functions to be used when converting from the spectral elements grid
to a gaussian grid
"""

import os
import glob
from cdo import Cdo, CDOException
import sai.common.locs as locs
# import netCDF4 as nc

DDIR = './data/'  # relative to this file
DDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), DDIR))
print('DDIR: ', DDIR)


# def set_outgrid(outtype='n360', method='con'):
#     global outtype
#     global method
#     outtype = outtype
#     method = method
#     return


def match_filenames(infile):
    """match input filename to existing files

    Parameters:
        infile (str | list of str): (list of) filename(s),
            may contain wildcards {'?','*'}

    Returns:
        matches: list of matching filenames
    """
    if isinstance(infile, str):
        matches = glob.glob(infile)
        print(f'found {len(matches)} matches to {infile}')
        return matches
    elif isinstance(infile, list):
        print('infile is a list of files!')
        matches = []
        for file in infile:
            matches.append(*match_filenames(file))
        return matches


def get_griddes(file):
    """Selects grid description from filename

    Parameters:
        file (str): filename

    Returns:
        griddes (str): grid description
    """
    fparts = os.path.basename(file).split('.')
    try:
        if fparts[-2] == 'grid':  # self made grid file
            griddes = fparts[0]
        else:  # original file
            griddes = fparts[4]
    except IndexError:
        print(f'cannot retrieve grid from {file}')
        raise
    return griddes


def set_stdname(infile):
    """Original files may not contain standard_name attributes for lon,lat
       This function ensures this is fixed.

    Parameters:
        infile (str): name of input file

    Returns:
        (str): copy of input file with standard_name attributes
    """
#    cdo = Cdo()  # new instance requiered as chaining does not work here
    ofile = os.path.join(DDIR, 'tmpfile')
    os.system('cdo setattribute,lon@standard_name=longitude,'
              f'lat@standard_name=latitude {infile} {ofile}')
#    return cdo.setattribute([lon@standard_name=longitude,
#                             lat@standard_name=latitude], infile)
    return ofile


def weight_fname(infile):
    """Create weight file name

    Parameters:
        infile (str): name of input file

    Returns:
        (str): name of remap weights file
    """
    inres = get_griddes(infile)
    basename = inres+'.to.'+outtype+'.'+method+'.nc'
    print(f'created wfname: {os.path.join(DDIR, basename)}')
    return os.path.join(DDIR, basename)


def create_weights(infile):
    """Create file with remap weights for faster grid conversion.

    Parameters:
        infile: (list of) filename(s), may contain wildcards {'*','?'}
    """
    if not os.path.isdir(DDIR):
        os.makedirs(DDIR)
        print(f'created folder {DDIR} for storing remap weights')
    for file in match_filenames(infile):
        inres = get_griddes(file)
        outfile = weight_fname(file)
        if os.path.isfile(outfile):
            print(f'file {outfile} already exists')
            return
        try:
            infile = set_stdname(infile)
            cdo = Cdo()
            if method == 'con':
                print('trying first order conservative remapping')
                cdo.gencon(outtype, input=file, output=outfile)
            elif method == 'con2':
                print('trying second order conservative remapping')
                cdo.gencon2(outtype, input=file, output=outfile)
        except CDOException:
            print('cdo failed')
            print(f'searching sample grid file in {DDIR}...')
            gridfiles = glob.glob(DDIR+inres+'.grid.nc')
            print(f'found: {gridfiles}')
            if len(gridfiles) == 0:
                print('found no grid file, quitting...')
                raise Exception
            elif len(gridfiles) == 1:
                print(f'using {gridfiles[0]} for regridding')
                create_weights(gridfiles[0])
            else:
                print('warning: found mutiple matches: using first')
                create_weights(gridfiles[0])
        else:  # no exception occurred
            print(f'remapping weigths stored in {outfile}')
        return


def main(infile):
    """Convert the grid according to existing weights, or create
       new weights if non-existing.

    Parameters:
        infile (str | list of str): (list of) filename(s),
            may contain wildcards {'?','*'}

    Returns:
        Xarray Dataset?
    """
    global outtype  # cdo type of output grid
    global method   # cdo regridding method
    outtype = 'n360'
    method = 'con'
    wfname = weight_fname(infile)
    if not os.path.isfile(wfname):
        print(f'creating default weights for {infile}')
        create_weights(infile)
    for file in match_filenames(infile):
        assert get_griddes(file) == os.path.basename(wfname).split('.')[0]
        print(f'found weight file for file {file}.\nRemapping now...')
        cdo = Cdo()
        ds = cdo.remap(outtype, wfname, input=file)
        print(f'remapped data to file {ds}')


if __name__ == '__main__':
    print(__file__)
#    create_weights(locs.HRMIP['U'])
    main(locs.HRMIP['U'])
#   create_weights('~/tas_cont000.nc')
