import logging
import os
import redis
import requests
import sys
import time
import threading
import traceback

# local modules
import barrier
import config
import file_saver
from TorCtl import TorCtl

# Constant
OUTPUT_DIR = "Others_v3"
TOR_PASSPHRASE = '123456'
NUM_NORMAL_THREADS=0
NUM_TOR_THREADS=7

# Set up logger
logger = logging.getLogger('crawl_linkedin')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler('output.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Timer
last_time_tor = 0

def set_last_time_tor(tmp):
    global last_time_tor    # Needed to modify global copy
    last_time_tor = tmp

def renew_tor_id():
    """ Send signal to TOR to renew identity """
    conn = TorCtl.connect(controlAddr="127.0.0.1", controlPort=9051, passphrase=TOR_PASSPHRASE)
    conn.send_signal("NEWNYM")


def update_cookie(headers, r):
    if 'set-cookie' in r.headers:
        headers['cookie'] = r.headers.get('set-cookie')


def worker(i, br):
    """ Each worker is put to 1 thread, consistently fetch 1 entry at a time from queue and process"""
    # Redis server
    redis_server = redis.Redis(config.REDIS_HOST)
    queue_item = redis_server.rpop(config.REDIS_CRAWLING_QUEUE)

    fsaver = file_saver.FileSaver(OUTPUT_DIR, i)

    num_url_crawled = 0

    if i < NUM_NORMAL_THREADS:
        headers = config.headers[0]
    else:
        headers = config.headers[1]
        proxies = {'http': '127.0.0.1:8118', 'https': '127.0.0.1:8118'}

    while queue_item:
        url_id, url = queue_item.split(' ', 1)

        try:
            if i < NUM_NORMAL_THREADS:
                r = requests.get(url, headers=headers, timeout=config.TIMEOUT_DURATION)
            else:
                r = requests.get(url, headers=headers, proxies=proxies, timeout=config.TIMEOUT_DURATION)
        except:
            err_trace = traceback.format_exc()
            logger.warning("Worker %d got exception:\n%s", i, err_trace)
            redis_server.lpush(config.REDIS_CRAWLED_RESULT_QUEUE, url_id + ' 3')
            queue_item = redis_server.rpop(config.REDIS_CRAWLING_QUEUE)
            continue

        if r.status_code >= 400 and r.status_code < 500:
            # Client Error
            logger.warning("Worker %d: URL %s returns with status code %d", i, url, r.status_code)
            crawl_status = 4

        elif r.status_code >= 500 and r.status_code < 600:
            # Server Error
            logger.warning("Worker %d: URL %s returns with status code %d", i, url, r.status_code)
            crawl_status = 5

        else:
            page_content = r.text

            num_url_crawled += 1

            if num_url_crawled % 100 == 0:
                logger.info("Worker %d crawled %d URLs", i, num_url_crawled)

            if len(page_content) < 8000 or r.status_code == 999:
                # Got blockes
                logger.info("Worker %d is blocked? URL : %s. Status code: %d", i, url, r.status_code)
                time.sleep(600)
                sys.exit(0)

                if i < NUM_NORMAL_THREADS:
                    logger.info("Worker %d sleeps for %d seconds", i, config.THREAD_SLEEP_DURATION)
                    time.sleep(config.THREAD_SLEEP_DURATION)
                    logger.info("Worker %d wakes up. Retry crawling", i)
                    update_cookie(headers, r)
                    continue
                else:
                    br.wait()
                    if i == NUM_NORMAL_THREADS:
                        logger.info("Worker %d renew TOR id", i)
                        renew_tor_id()
                        r = requests.get('http://www.linkedin.com/', proxies=proxies, timeout=config.TIMEOUT_DURATION)
                        update_cookie(headers, r)                    
                    br.wait()
                    continue

            elif len(page_content) < 40000:
                #logger.info("Worker %d: Not enough info %s. Status code: %d", i, url, r.status_code)
                crawl_status = 2
                update_cookie(headers, r)
        
            else:
                # Download successfully
                try:
                    file_path = fsaver.save_file_to_disk(url, page_content, url_id)
                    
                    if i>=3:
                         set_last_time_tor(time.time())
                    else:
                         #print str(time.time()-last_time_tor)
                         if (time.time()-last_time_tor) > 600:
                            sys.exit(0)
                            
                    crawl_status = 1
                except:
                    err_trace = traceback.format_exc()
                    logger.warning("Worker %d got exception:\n%s", i, err_trace)
                    sys.exit(0)
                    crawl_status = 3                   
            
                update_cookie(headers, r)

        redis_server.lpush(config.REDIS_CRAWLED_RESULT_QUEUE, url_id + ' ' + str(crawl_status))
        queue_item = redis_server.rpop(config.REDIS_CRAWLING_QUEUE)

    print "Num URL crawled by worker: " + str(i) + ": " + str(num_url_crawled)    


def main():
    # Check output dir
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    elif not os.path.isdir(OUTPUT_DIR):
        raise Exception(OUTPUT_DIR + " is not a directory")
	
    br = barrier.Barrier(NUM_TOR_THREADS)    
	
    for i in range(NUM_NORMAL_THREADS + NUM_TOR_THREADS):
        t = threading.Thread(target=worker, args=(i, br))
        t.daemon = True
        t.start()

    while threading.active_count() > 1:
        time.sleep(1)

if __name__ == '__main__':
    start_time = time.time()
    set_last_time_tor(time.time())
    main()
    elapsed_time = time.time() - start_time
    print "Elapsed time: " + str(elapsed_time) + " seconds"
