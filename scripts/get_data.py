from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm
import requests


def __download(src, dst):
    '''
    Downloader with progress bar. Not meant to be used directly
    '''
    r = requests.get(src)
    if r.ok:
        chunk_size = 1024
        with open(dst, 'wb') as out_file:
            for chunk in tqdm(r.iter_content(chunk_size), desc=dst.name, unit='KB', unit_divisor=1024, unit_scale=True):
                out_file.write(chunk)
    else:
        return 'Failed | %s | HTTP-%d: %s' % (dst.name, r.status_code, r.reason)
    return 'Complete'

def download_hls_data(tiles, output_dir, max_download_threads=2):
    '''
    Using <max_download_threads> concurrent threads (Default 2), 
    download the desired tile files to <output_dir>
    '''
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
