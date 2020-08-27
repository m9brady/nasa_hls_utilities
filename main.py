import sys
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.get_data import download_hls_data, generate_tile_names
from scripts.image_tools import hdf_to_tif, merge_tifs, add_overviews, acceptable_cloud_coverage

DOWNLOAD_DIR = Path(__file__).parent / 'hls_downloads'
TILES = Path(__file__).parent / 'assets' / 'tiles.json'
CLEANUP = True

def main(args):
    '''
    Usage:
        python main.py <aoi defined in scripts/tiles.json> <YYYYmmDD>

    Example:
        python main.py Eureka 20200720
    '''
    area = args[1]
    dt = args[2]
    with TILES.open() as f:
        data = json.load(f)
    aoi = data.get(area, 'Eureka')
    tiles = generate_tile_names(aoi, dt)
    download_hls_data(tiles, DOWNLOAD_DIR)
    hdfs = list(DOWNLOAD_DIR.glob('*.hdf'))
    if len(hdfs) == 0:
        print('No data found for date %s' % dt)
        return
    '''
    # check cloud coverage
    good = [acceptable_cloud_coverage(hdf) for hdf in hdfs]
    if not all(good):
        print('Too much cloud coverage for:')
        print('\n'.join([hdfs[idx].name for idx, i in enumerate(good) if not i]))
        if CLEANUP:
            [hdf.unlink() for hdf in hdfs]
        return
    '''
    print('Converting HDF to GTiff...')
    tifs = [hdf_to_tif(hdf) for hdf in hdfs]
    mosaic_file = DOWNLOAD_DIR / ('HLS.%s.%s.%s.v%s.tif' % (
        tifs[0].stem.split('.')[1],
        area,
        tifs[0].stem.split('.')[3],
        tifs[0].stem.split('v')[-1]
    ))
    print('Building Mosaic...')
    merge_tifs(tifs, mosaic_file)
    print('Embedding Image Overviews...')
    add_overviews(mosaic_file)
    print('Cleaning up...')
    if CLEANUP:
        [hdf.unlink() for hdf in hdfs]
        [tif.unlink() for tif in tifs]
        [reproj.unlink() for reproj in DOWNLOAD_DIR.glob('*_reproj.tif')]
    print('Mosaic file ready: %s' % mosaic_file)
    return

if __name__ == '__main__':
    main(sys.argv)

