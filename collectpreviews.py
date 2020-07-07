import argparse
import os
import time
import urllib.request
from sys import argv
from urllib.error import HTTPError, URLError

from PIL import Image

# from pprint import pprint as pp


def parse_arguments():
    """Parse the argiments of command line."""
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
    print(f'\nDownload from: {url}')
    try:
        response = urllib.request.urlopen(url)
    except (URLError, HTTPError):
        return
    else:
        # print(response.info())
        image_type = response.info()['Content-Type']
        print(f'Image type: {image_type}')
        print(f'Bytes: {response.info()["Content-Length"]}')
        print(type(response))
        return response


def make_thumbnail(image_raw, size):
    print('Convert image')
    try:
        image = Image.open(image_raw)
    except IOError:
        print('Can\'t open the image!\n')
        return
    else:
        print(image.size)
        image.thumbnail(size)
        print(image.size)
        return image.convert(mode='RGB')


def main():
    start_time = time.perf_counter()
    if len(argv) == 1:
        print('The file of urls is required. Run "collectpreviews.py --help"')
        return

    args = parse_arguments()
    source_file = args.file
    # threads = args.threads
    size = tuple(int(x) for x in args.size.split('x'))
    current_dir = os.getcwd()
    output_dir = os.path.join(current_dir, args.dir)
    if not os.path.exists(output_dir) or not os.path.isdir(output_dir):
        try:
            os.mkdir(output_dir)
        except OSError:
            print(f'Can\'t create output directory {output_dir}')
            return

    with open(source_file, 'r') as f:
        urls = [x.strip('\n') for x in f.readlines()]
    # pp(urls)

    total_urls = len(urls)
    total_bytes = 0
    created_file_num = 0
    total_errors = 0
    for i, url in enumerate(urls):
        image_raw = download_image(url)
        if not image_raw:
            print('Something was wrong!\n')
            total_errors += 1
            continue
        total_bytes += int(image_raw.info()['Content-Length'])
        thumbnail = make_thumbnail(image_raw, size)
        if not thumbnail:
            print('Something was wrong!\n')
            total_errors += 1
            continue
        print(thumbnail.info)

        new_name = f'{i:05d}.jpeg'
        full_path = os.path.join(output_dir, new_name)
        # print(full_path)
        try:
            thumbnail.save(full_path, 'jpeg')
        except (KeyError, IOError):
            print('Can\'t save file')
            total_errors += 1
        else:
            print(f'Created {new_name} from {url}')
            created_file_num += 1

    print(f'From {total_urls} urls created {created_file_num} files.\n',
          f'Downloaded {total_bytes} bytes.\n',
          f'{total_errors} errors occured.\n',
          f'Working time: {time.perf_counter() - start_time} s.')


if __name__ == '__main__':
    main()
