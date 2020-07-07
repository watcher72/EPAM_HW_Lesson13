import argparse
# import logging
import logging.config
import os
import time
import urllib.request
from sys import argv
from urllib.error import HTTPError, URLError

from PIL import Image

# from pprint import pprint as pp


EXCEPT_FORMAT = ('%(asctime)s - %(funcName)s - %(name)s'
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
        'deblogger': {'handlers': ['consoleDebugHandler'], 'level': 'DEBUG'},
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


def download_image(url):
    try:
        response = urllib.request.urlopen(url)
    except (URLError, HTTPError):
        ex_log.exception(f'Some error with downloading from {url}!!')
        return
    else:
        # print(response.info())
        image_type = response.info()['Content-Type']
        d_log.debug(f'Image type: {image_type}')
        d_log.debug(f'Bytes: {response.info()["Content-Length"]}')
        return response


def make_thumbnail(image_raw, size):
    d_log.debug('Convert image')
    try:
        image = Image.open(image_raw)
    except IOError:
        ex_log.exception('Some error on making thumbnail!\n')
        return
    else:
        d_log.debug(image.size)
        image.thumbnail(size)
        d_log.debug(image.size)
        return image.convert(mode='RGB')


def main():
    if len(argv) == 1:
        print('The file of urls is required. Run "collectpreviews.py --help"')
        return

    start_time = time.time()
    d_log.debug(f'\nProgram started at {time.ctime(start_time)}')

    args = parse_arguments()
    source_file = args.file
    # threads = args.threads
    try:
        size = tuple(int(x) for x in args.size.split('x'))
    except ValueError:
        print('Size must be in format "<width>x<height>".')
        return

    current_dir = os.getcwd()
    output_dir = os.path.join(current_dir, args.dir)
    if not os.path.exists(output_dir) or not os.path.isdir(output_dir):
        try:
            os.mkdir(output_dir)
        except OSError:
            ex_log.exception(f'Can\'t create output directory {output_dir}')
            return

    with open(source_file, 'r') as f:
        urls = [x.strip('\n') for x in f.readlines()]
    # pp(urls)

    total_urls = len(urls)
    total_bytes = 0
    count_created_file = 0
    errors = 0
    for i, url in enumerate(urls):
        d_log.debug(f'\nWorking on: {url}')
        image_raw = download_image(url)
        if not image_raw:
            d_log.debug(f'Can\'t download image from {url}\n')
            errors += 1
            continue
        total_bytes += int(image_raw.info()['Content-Length'])
        thumbnail = make_thumbnail(image_raw, size)
        if not thumbnail:
            d_log.debug(f'Can\'t make thumbnail from {url}\n')
            errors += 1
            continue

        new_name = f'{i:05d}.jpeg'
        full_path = os.path.join(output_dir, new_name)
        # print(full_path)
        try:
            thumbnail.save(full_path, 'jpeg')
        except (KeyError, IOError):
            ex_log.exception(f'Can\'t save thumbnail file from {url}')
            errors += 1
        else:
            print(f'Created {new_name} from {url}')
            count_created_file += 1

    finish_time = time.time()
    print(f'From {total_urls} urls created {count_created_file} files.\n',
          f'Downloaded {total_bytes} bytes.\n',
          f'{errors} {"error" if errors == 1 else "errors"} occured.\n',
          f'Working time: {finish_time - start_time} s.')

    d_log.debug(f'\nProgram finished at {time.ctime(finish_time)}\n')


if __name__ == '__main__':
    main()
