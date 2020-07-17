from pathlib import Path
from statistics import mode

import rasterio as rio
import xarray as xr
from pyproj.crs import CRS
from rasterio.enums import ColorInterp, Resampling
from rasterio.merge import merge

UTM_CRS = {
    '16': CRS.from_epsg(32616), # UTM 16N WGS84 # Eureka
    '15': CRS.from_epsg(32615), # UTM 15N WGS84 # Eureka
    '08': CRS.from_epsg(32608), # UTM 8N WGS84 # TVC
}

SAMPLE_METHODS = {
    'nearest': Resampling.nearest,
    'cubic': Resampling.cubic,
    'average': Resampling.average,
    'cubic-spline': Resampling.cubic_spline,
    'gauss': Resampling.gauss,
    'mode': Resampling.mode,
    'lanczos': Resampling.lanczos
}

def acceptable_cloud_coverage(hdf_file, threshold=30):
    '''
    If the given HLS dataset has too much cloud coverage as defined
    by threshold (default: 30%), return False
    '''
    with xr.open_dataset(hdf_file) as ds:
        if ds.attrs.get('cloud_coverage') > threshold:
            return False
        else:
            return True

def hdf_to_tif(hdf_file):
    '''
    Produce an RGB, tiled, compressed GeoTIFF from a given HLS HDF4 granule
    '''
    if not isinstance(hdf_file, Path):
        hdf_file = Path(hdf_file)
    tif_file = hdf_file.parent / (hdf_file.stem + '.tif')
    if tif_file.exists():
        print('Warning - GTiff exists for %s' % hdf_file.name)
    hls_type = hdf_file.stem.split('.')[1][0]
    crs = UTM_CRS.get(hdf_file.stem.split('.')[2][1:3])
    # based on https://hls.gsfc.nasa.gov/wp-content/uploads/2019/01/HLS.v1.4.UserGuide_draft_ver3.1.pdf
    # L30 products
    if hls_type == 'L':
        red = ':Grid:band04'
        green = ':Grid:band03'
        blue = ':Grid:band02'
    # S30 products
    elif hls_type == 'S':
        red = ':Grid:B04'
        green = ':Grid:B03'
        blue = ':Grid:B02'
    sds_list = [f'HDF4_EOS:EOS_GRID:{str(hdf_file)}{band}' for band in [red, green, blue]]
    # copy the metadata from the red-band as a template
    with rio.open(sds_list[0]) as src:
        meta = src.meta.copy()
    # update the metadata to add some multiband/compression/tiling
    meta.update({
        'count': len(sds_list),
        'crs': crs,
        'driver': 'GTiff',
        'dtype': 'int16',
        'compress': 'DEFLATE',
        'tiled': True,
        'tilexsize': 256,
        'tileysize': 256,
        'nodata': None
    })
    # write 3-band tif
    with rio.open(tif_file, 'w', **meta) as dst:
        for band_id, band in enumerate(sds_list, start=1):
            with rio.open(band) as src:
                dst.write_band(band_id, src.read(1))
        dst.colorinterp = [ColorInterp.red, ColorInterp.green, ColorInterp.blue]
    return 

def merge_tifs(tifs, mosaic_file):
    '''
    '''
    open_tifs = [rio.open(tif) for tif in tifs]
    # establish common crs
    common_crs = mode([tif.crs for tif in open_tifs])
    # mosaic
    mosaic, transform = merge(open_tifs)
    # copy metadata from first tif
    meta = open_tifs[0].meta.copy()
    meta.update({
        'height': mosaic.shape[1],
        'width': mosaic.shape[2],
        'transform': transform,
        'crs': common_crs,
        'compress': 'DEFLATE',
        'tiled': True,
        'tilexsize': 256,
        'tileysize': 256,
        'nodata': None
    })
    with rio.open(mosaic_file, 'w', **meta) as dst:
        dst.write(mosaic)
        dst.colorinterp = [ColorInterp.red, ColorInterp.green, ColorInterp.blue]
    return

def add_overviews(tif, zlevels=[2, 4, 6, 12], resample_method='cubic'):
    '''
    For a given GeoTIFF, generate internal overviews for 
    the provided zoom_levels using the provided resample method
    '''
    try:
        method = SAMPLE_METHODS.get(resample_method)
    except KeyError:
        print('Method "%s" not supported' % resample_method)
        return
    with rio.open(tif, 'r+') as ds:
        ds.build_overviews(zlevels, method)
        ds.update_tags(ns='rio_overview', resampling=resample_method)
    return
