import queue
import time
import threading
from multiprocessing.pool import ThreadPool


data = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
        'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen',
        'sixteen', 'seventeen', 'eighteen', 'nineteen', 'twenty', 'twenty-one',
        'twenty-two', 'twenty-three', 'twenty-four', 'twenty-five',
        'twenty-six', 'twenty-seven', 'twenty-eight', 'twenty-nine']


q = queue.Queue()
lock_counter = threading.Lock()
lock_errors = threading.Lock()

pool = ThreadPool(8)
pool_handle = ThreadPool(8)

total_length = 0
count_downloaded = 0
count_created = 0
count_errors = 0

urls = [(i, item) for i, item in enumerate(data)]
# print(urls)


def download_item(index, url):
    global total_length
    global count_downloaded
    time.sleep(1)
    with lock_counter:
        total_length += len(url)
        count_downloaded += 1
        print(f'Put {url}')
    q.put((index, url))


def print_queue_item():
    global count_created
    global count_errors
    while not q.empty():
        index, item = q.get()
        print(f'Get {item}')
        with lock_counter:
            count_created += 1
        with lock_errors:
            if index % 2:
                count_errors += 1
            print(f'{index}_{item.upper()}')
        q.task_done()


start = time.perf_counter()
pool.starmap(download_item, urls)
pool_handle.apply(print_queue_item)
# threading.Thread(target=print_queue_item()).start()

q.join()

finish = time.perf_counter()
print(f'Pool with queue worked {finish - start:.2f} s')
print(f'Downloaded {count_downloaded}')
print(f'Created {count_created}')
print(f'Errors {count_errors}')
print(f'Total lenght {total_length}\n')


count_downloaded = 0
count_created = 0
count_errors = 0


def handle_url(item):
    global count_downloaded
    global count_errors
    index, url = item
    with lock_counter:
        count_downloaded += 1
        count_errors += 1
        print(index, url.upper())
    time.sleep(1)
    return f'{index}_{url.upper()}'


pool_1 = ThreadPool(8)

start1 = time.perf_counter()
result = pool_1.map(handle_url, urls)
finish1 = time.perf_counter()
print(f'Pool worked {finish1 - start1:.2f} s')
# print(result)
print(count_downloaded, count_errors)

# start2 = time.perf_counter()
# for item in urls:
#     handle_url(item)
# finish2 = time.perf_counter()
# print(f'Loop worked {finish2 - start2}')
# print(result)
