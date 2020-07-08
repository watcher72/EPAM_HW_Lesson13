"""
Collect the thumbnails of the images from the urls given in the file
(one url per row).
"""
import argparse
import logging.config
import os
import threading
import time
import queue
import urllib.request
from multiprocessing.pool import ThreadPool
from sys import argv
from typing import Tuple
from urllib.error import HTTPError, URLError

from PIL import Image

# from pprint import pprint as pp

# Define logging configuration
EXCEPT_FORMAT = ('\n%(asctime)s - %(funcName)s - %(name)s'
                 ' - %(levelname)s - %(message)s')
DEBUG_FORMAT = '%(message)s'
LOG_CONFIG = {
    'version': 1,
    'handlers': {
        'consoleDebugHandler': {
            'class': 'logging.StreamHandler', 'formatter': 'mesFormatter',
            'level': 'DEBUG'
        },
        'fileDebugHandler': {
            'class': 'logging.FileHandler', 'formatter': 'mesFormatter',
            'filename': 'debug.log', 'level': 'DEBUG'
        },
        'exceptHandler': {
            'class': 'logging.FileHandler', 'formatter': 'exFormatter',
            'filename': 'except.log', 'level': 'ERROR'
        }
    },
    'loggers': {
        'deblogger': {'handlers': ['fileDebugHandler'], 'level': 'DEBUG'},
        'exceptLogger': {'handlers': ['exceptHandler'], 'level': 'ERROR'}
    },
    'formatters': {
        'mesFormatter': {'format': DEBUG_FORMAT},
        'exFormatter': {'format': EXCEPT_FORMAT,
                         'datefmt': '%Y.%m.%d %H:%M:%S'}
    }
}

logging.config.dictConfig(LOG_CONFIG)
d_log = logging.getLogger('deblogger')
ex_log = logging.getLogger('exceptLogger')


lock_err = threading.Lock()
lock_byte_counter = threading.Lock()
lock_created_files = threading.Lock()

q = queue.Queue()


count_downloaded = 0
total_bytes = 0
count_created_file = 0
errors = 0


def parse_arguments():
    """Parse the arguments of command line."""
    parser = argparse.ArgumentParser(
        description='Save the thumbnails of the images from urls'
    )
    parser.add_argument('file', type=str, help='File with urls')
    parser.add_argument('--dir', '-d', type=str, default='',
                        help='Output directory')
    parser.add_argument('--threads', '-t', type=int, default=1,
                        help='Number of threads')
    parser.add_argument('--size', '-s', type=str, default='100x100',
                        help='Size of thumbnails')

    args = parser.parse_args()
    return args


def download_image(index: int, url: str) -> None:
    """
    Download the image from given url.

    :param index: the order number of url in given file
    :param url: url for download the inage
    :return: tuple with order number of url and raw data of image
    """
    global count_downloaded
    global total_bytes
    global errors

    d_log.debug(f'\nWorking on file {index} from: {url}')
    try:
        response = urllib.request.urlopen(url)
    except (URLError, HTTPError):
        d_log.debug(f'Can\'t download image from {url}\n')
        with lock_err:
            errors += 1
        ex_log.exception(f'Some error with downloading from {url}!!')
        return

    with lock_byte_counter:
        d_log.debug(f'Image type: {response.info()["Content-Type"]}')
        d_log.debug(f'Bytes: {response.info()["Content-Length"]}')
        count_downloaded += 1
        total_bytes += int(response.info()['Content-Length'])
    # print(f'Put {index} in queue')
    q.put((index, response))


def make_thumbnail(image_raw, size: Tuple[int, int]):
    """
    Create a thumbnail of given raw data of image.

    :param image_raw: raw data of image
    :param size: thumbnail's size
    :return: thumbnail's Pillow object
    """
    try:
        image = Image.open(image_raw)
    except IOError:
        ex_log.exception('\nSome error on making thumbnail!\n')
        return

    d_log.debug(image.size)
    image.thumbnail(size)
    d_log.debug(image.size)
    return image.convert(mode='RGB')


def save_thumbnail(output_dir, size) -> None:
    """
    Save thumbnail of the image on the given directory.

    :param output_dir: directory in which must be saved the thumbnail
    :param size: thumbnail's size
    :return: None
    """
    global errors
    global count_created_file

    while not q.empty():
        index, image_raw = q.get(block=True)
        # print(f'Get {index} from queue')
        d_log.debug(f'Convert image {index}')
        thumbnail = make_thumbnail(image_raw, size)
        if not thumbnail:
            d_log.debug(f'Can\'t make thumbnail {index}\n')
            with lock_err:
                errors += 1
            return

        new_name = f'{index:05d}.jpeg'
        full_path = os.path.join(output_dir, new_name)
        # print(full_path)
        try:
            thumbnail.save(full_path, 'jpeg')
        except (KeyError, IOError):
            ex_log.exception(f'\nCan\'t save thumbnail file {index}')
            with lock_err:
                errors += 1
        else:
            with lock_created_files:
                count_created_file += 1
                print(f'Created {new_name}')
        q.task_done()


def main():
    if len(argv) == 1:
        print('The file of urls is required. Run "collectpreviews.py --help"')
        return

    start_time = time.time()
    d_log.debug(f'\nFull-work in one Pool program started at {time.ctime(start_time)}')

    # Parse arguments
    args = parse_arguments()
    source_file = args.file
    threads = args.threads
    try:
        thumb_size = tuple(int(x) for x in args.size.split('x'))
    except ValueError:
        print('Size must be in format "<width>x<height>".')
        return

    current_dir = os.getcwd()
    output_dir = os.path.join(current_dir, args.dir)
    if not os.path.exists(output_dir) or not os.path.isdir(output_dir):
        try:
            os.mkdir(output_dir)
        except OSError:
            ex_log.exception(f'\nCan\'t create output directory {output_dir}')
            return

    # Read urls from file
    with open(source_file, 'r') as f:
        urls = [x.strip('\n') for x in f.readlines()]
    # urls = [(i, url) for i, url in enumerate(urls)]
    # pp(urls)

    total_urls = len(urls)

    pool_download = ThreadPool(threads)
    pool_save = ThreadPool(threads)

    pool_download.starmap(download_image,
                          [(i, url) for i, url in enumerate(urls)])
    # threading.Thread(target=save_thumbnail, args=(output_dir, thumb_size))
    pool_save.apply(save_thumbnail, (output_dir, thumb_size))

    q.join()

    finish_time = time.time()
    print(f'\nFrom {total_urls} urls dowmloaded {count_downloaded} files\n',
          f'Downloaded {total_bytes} bytes.\n',
          f'Created {count_created_file} files.\n',
          f'{errors} {"error" if errors == 1 else "errors"} occurred.\n',
          f'Working time: {finish_time - start_time:.2f} s.')

    d_log.debug(f'\nWorking time: {finish_time - start_time:.2f} s.\n')


if __name__ == '__main__':
    main()
