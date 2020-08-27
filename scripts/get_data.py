from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tqdm import tqdm
import requests


def generate_tile_names(tiles, date, product='S30', version='1.4'):
    '''
    Given tiles and desired date, return the proper filenames for grabbing
    '''
    date = date.replace('-', '').replace('/','')
    # assume YYYYJJJ first
    if len(date) == 7:
        try:
            date = datetime.strptime(date, '%Y%j').strftime('%Y%j')
        except ValueError:
            date = datetime.strptime(date, '%Y%m%d').strftime('%Y%j')
    # YYYYMMDD
    elif len(date) == 8:
        date = datetime.strptime(date, '%Y%m%d').strftime('%Y%j')
    # check product
    if not product in ['S30', 'L30']:
        raise NotImplementedError('Product %s not recognized')
    return [
        f'HLS.{product}.{tile}.{date}.v{version}.hdf'
        for tile in tiles
    ]

def __download(src, dst):
    '''
    Downloader with progress bar. Not meant to be used directly
    '''
    r = requests.get(src)
    if r.ok:
        chunk_size = 1024**2
        with open(dst, 'wb') as out_file:
            for chunk in tqdm(r.iter_content(chunk_size), desc=dst.name, unit='MB'):
                out_file.write(chunk)
    else:
        return 'Failed | %s | HTTP-%d: %s' % (dst.name, r.status_code, r.reason)
    return 'Complete'

def download_hls_data(tiles, output_dir, max_download_threads=2):
    '''
    Using <max_download_threads> concurrent threads (Default 2), 
    download the desired tile files to <output_dir>
    '''
    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)
    download_urls = [
        'https://hls.gsfc.nasa.gov/data/v1.4/%s/%s' % (
            tile.split('.')[1],
            '/'.join([
                tile.split('.')[3][:4],
                tile.split('.')[2][1:3],
                tile.split('.')[2][3],
                tile.split('.')[2][4],
                tile.split('.')[2][5],
                tile
            ])
        )
        for tile in tiles
    ]
    local_files = [output_dir / tile for tile in tiles]
    executor = ThreadPoolExecutor(max_workers=max_download_threads)
    futures = [executor.submit(__download, remote, local) for remote, local in zip(download_urls, local_files)]
    results = [f.result() for f in futures]
    failures = [f for f in results if f != 'Complete']
    if len(failures) > 0:
        print('Errors encountered for tiles:\n%s' % '\n'.join(failures))
    return
