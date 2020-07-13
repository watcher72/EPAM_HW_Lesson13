import queue
import random
import time
import threading
from collections import deque
from multiprocessing.pool import ThreadPool


data = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
        'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen',
        'sixteen', 'seventeen', 'eighteen', 'nineteen', 'twenty', 'twenty-one',
        'twenty-two', 'twenty-three', 'twenty-four', 'twenty-five',
        'twenty-six', 'twenty-seven', 'twenty-eight', 'twenty-nine']

urls = [(i, item) for i, item in enumerate(data)]
# print(urls)

threads = 8


# ----------------------------------------------
# ThreadPool with queue and threading.Condition
# ----------------------------------------------

q = deque()
is_empty = threading.Condition()

result_cond = []


def producer(index, url):
    is_empty.acquire()
    print(f'Put {url.upper()} in queue')
    time.sleep(random.random())
    q.append((index, url.upper()))
    is_empty.notify()
    is_empty.release()


def consumer():
    while True:
        is_empty.acquire()
        # здесь каким-то образом надо определить, что ThreadPool
        # закончил свою работу и очередь пуста, и выйти из while
        # if ThreadPool has finished and not q:
        #     break
        while not q:
            is_empty.wait()
        index, item = q.popleft()
        print(f'Get {item} from queue')
        print(f'{index}_{item}')
        result_cond.append(f'{index}_{item}')
        is_empty.release()


start_cond = time.time()

t_consumers = []
for _ in range(threads):
    t = threading.Thread(target=consumer)
    t_consumers.append(t)
    t.start()

pool_producer = ThreadPool(threads)
pool_producer.starmap(producer, urls)

# pool_consumer = ThreadPool(threads)
# pool_consumer.apply(consumer)

for t in t_consumers:
    t.join()

# И вот сюда мы вообще попасть не можем, так как все cunsumers
# продолжают крутиться в бесконечных циклах
print(result_cond)
finish_cond = time.time()
print(f'Pool with Condition worked {finish_cond - start_cond:.2f} s')


# ---------------------------
# ThreadPool vs. simple loop
# ---------------------------

# count_downloaded = 0
# count_created = 0
# count_errors = 0
#
#
# def handle_url(item):
#     global count_downloaded
#     global count_errors
#     index, url = item
#     with lock_counter:
#         count_downloaded += 1
#         if index % 2:
#             count_errors += 1
#         print(index, url.upper())
#     time.sleep(1)
#     return f'{index}_{url.upper()}'
#
#
# pool_1 = ThreadPool(threads)
#
# start1 = time.perf_counter()
# result = pool_1.map(handle_url, urls)
# finish1 = time.perf_counter()
# print(f'Pool worked {finish1 - start1:.2f} s')
# # print(result)
# print(count_downloaded, count_errors)


# start2 = time.perf_counter()
# for item in urls:
#     handle_url(item)
# finish2 = time.perf_counter()
# print(f'Loop worked {finish2 - start2}')
# print(result)


# ---------------------------------
# Two ThreadPools with queue.Queue
# ---------------------------------

# q = queue.Queue()
# lock_counter = threading.Lock()
# lock_errors = threading.Lock()
#
# pool = ThreadPool(threads)
# pool_handle = ThreadPool(threads)
#
# total_length = 0
# count_downloaded = 0
# count_created = 0
# count_errors = 0
#
#
# def download_item(index, url):
#     global total_length
#     global count_downloaded
#     time.sleep(1)
#     with lock_counter:
#         total_length += len(url)
#         count_downloaded += 1
#         print(f'Put {url}')
#     q.put((index, url))
#
#
# def print_queue_item():
#     global count_created
#     global count_errors
#     while not q.empty():
#         index, item = q.get()
#         print(f'Get {item}')
#         with lock_counter:
#             count_created += 1
#         with lock_errors:
#             if index % 2:
#                 count_errors += 1
#             print(f'{index}_{item.upper()}')
#         q.task_done()
#
#
# start = time.perf_counter()
# pool.starmap(download_item, urls)
# pool_handle.apply(print_queue_item)
# # threading.Thread(target=print_queue_item()).start()
#
# q.join()
#
# finish = time.perf_counter()
# print(f'Pool with queue worked {finish - start:.2f} s')
# print(f'Downloaded {count_downloaded}')
# print(f'Created {count_created}')
# print(f'Errors {count_errors}')
# print(f'Total lenght {total_length}\n')


# ----------------------------------------------
# some tests with ThreadPoolExecutor
# ----------------------------------------------

# from concurrent.futures import ThreadPoolExecutor
#
# q = deque()
# is_empty = threading.Condition()
#
# result_cond = []
# t_consumers = []
# t_producers = []
#
# urls_iter = iter(urls)
#
# done = False
#
#
# def producer(item):
#     index, url = item
#     is_empty.acquire()
#     print(f'Put {url.upper()} in queue')
#     time.sleep(random.random())
#     q.append((index, url.upper()))
#     is_empty.notify()
#     is_empty.release()
#
#
# def consumer():
#     while True:
#         is_empty.acquire()
#         # if not q and done:
#         #     break
#         while not q:
#             is_empty.wait()
#         index, item = q.popleft()
#         print(f'Get {item} from queue')
#         print(f'{index}_{item}')
#         result_cond.append(f'{index}_{item}')
#         is_empty.release()
#
#
# # def sign_done(some):
# #     global done
# #     done = True
# #     print(some)
#
#
# start_cond = time.time()
#
# for _ in range(threads):
#     t = threading.Thread(target=consumer)
#     t_consumers.append(t)
#     t.start()
#
# with ThreadPoolExecutor(max_workers=threads) as executor:
#     f = executor.map(producer, urls)
#
# # f.add_done_callback(sign_done)
#
# for t in t_consumers:
#     t.join()
#
# print(result_cond)
#
# finish_cond = time.time()
# print(f'Pool with Condition worked {finish_cond - start_cond:.2f} s')
