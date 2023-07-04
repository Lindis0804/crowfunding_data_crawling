from n_kafka.producer import ProjectProducer
import selenium.common.exceptions
import urllib3.exceptions
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from time import sleep
import threading
import csv
import traceback
import multiprocessing
from typing import Union
import os
from dotenv import load_dotenv
from datetime import datetime
import random
import json
import requests
import pytz
# import tzdata


def round_number(s):
    return s - s % (10**(len(str(s))-1))


def right_trim(lsts):
    ls = lsts
    le = len(ls)
    for i in range(le-1, -1, -1):
        if (ls[i] == ''):
            ls.pop(i)
        else:
            break
    return ls


def split_list(lst, k):
    """
    This function is used to split a list to sub-lists that have k items.

    Args:
        lst (list): input list
        k (int): number of item of a sub-list

    Returns:
        list: list of sub-list
    For example:
    split_list([1,2,3,4,5],2) => [[1,2],[3,4],[5]]
    """
    size = len(lst)
    return [lst[i:i+k] for i in range(0, size, k)]


# print(split_list(["hi", 2, 3, 4, 5], 3))
# print([1, 2, 3, 4, 5, 6][:-1])
def get_first_not_null_item(lsts):
    """
      this function is used to get the first not null item of a list, it there is no not-null item,
      this func returns the last item

    Args:
        lsts (list): input list

    Returns:
        string: the first not null item
    """
    if (len(lsts) == 0):
        return ""
    for l in lsts:
        if (len(l) != 0):
            return l
    return lsts[-1]


def set_up_browser():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("disable-blink-features")
    # chrome_options.add_argument("disable-blink-features=AutomationControlled")
    # cái sand box này làm cho docker bị treo
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--enable-javascript")
    chrome_options.add_argument("--enable-cookies")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--log-level=3')
    # chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument("--disable-extensions")
    # chrome_options.add_argument("--disable-3d-apis")
    # chrome_options.add_argument("--proxy-server='direct://'")
    # chrome_options.add_argument("--proxy-bypass-list=*")
    # chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-running-insecure-content')
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    # way 1:
    browser = webdriver.Chrome(
        service=(Service(ChromeDriverManager().install())), options=chrome_options
    )

    # way 2:
    # service = Service(executable_path='./driver/chromedriver')
    # browser = webdriver.Chrome(
    #     service=service, options=chrome_options)

    return browser


def save_to_json_file(file_addr, data):
    with open(file_addr, "r") as jf:
        cur_data = json.load(jf)
    if (isinstance(cur_data, list)):
        cur_data.append(data)
    elif (isinstance(cur_data, dict)):
        cur_data = data
    with open(file_addr, "w") as jf:
        json.dump(cur_data, jf)


def save_to_file(file_addr, content, mode="a"):
    file = open(file_addr, mode)
    file.write(json.dumps(content))
    file.write("\n")


def save_to_mongodb(data, db_name="local", collection_name="kickstarter_err_url"):
    try:
        client = MongoClient(os.environ.get("mongodb"))
        db = client[db_name]
        collection = db[collection_name]
        result = collection.insert_one(data)
        client.close()
    except:
        traceback.print_exc()


def get_detail_project(page, url, error_url_file="./data/err_url.csv", producer=[], web_driver_wait=5, delay_time=0.5):
    """
    This func is used to crawl data of a detail project page of Kickstarter website

    Args:
        page (int): page index of url
        url (string): link to website that need to be crawled
        error_url_file (string): name of file that contains a list of error urls that can not be crawled (
            this list will be executed later
        )
    """
    browser = set_up_browser()
    sleep(delay_time)
    try:
        print("crawl url: ")
        print(url)
        browser.get(url)
        wait = WebDriverWait(browser, web_driver_wait)
        # get title
        titles = browser.find_elements(
            By.CSS_SELECTOR, "h2.type-24-md.soft-black.mb1.project-name")
        if (len(titles) == 0):
            raise Exception("Access denied.")
        title = get_first_not_null_item(list(map(lambda a: a.text, titles)))

        # get description
        description = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "p[class='type-14 type-18-md soft-black project-description mb1']"))))

        # get picture link
        picture = get_first_not_null_item(list(map(lambda a: a.get_attribute("src"), browser.find_elements(
            By.CSS_SELECTOR, "img[class='aspect-ratio--object bg-black z3']"))))

        # get pledged
        pledged = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "span[class='ksr-green-500']"))))

        # get goal
        goals = list(map(lambda x: x.text, browser.find_elements(
            By.CSS_SELECTOR, "span[class='inline-block-sm hide'] > span[class='money']")))
        goal = get_first_not_null_item(goals)
        print("goal", goal)
        # get num of backer
        num_of_backer = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "div[class='block type-16 type-28-md bold dark-grey-500']"))))

        # get days to go
        days_to_go = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "span[class='block type-16 type-28-md bold dark-grey-500']"))))

        # get mark, field, location
        mark_field_locations = right_trim(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "a.nowrap.navy-700.flex.items-center.medium.mr3.type-12.keyboard-focusable > span.ml1"
        ))))
        print("mark_field_location", mark_field_locations)
        if (len(mark_field_locations) < 3):
            try:
                field = mark_field_locations[0]
            except:
                field = ''
            try:
                location = mark_field_locations[1]
            except:
                location = ''
            mark = ''
        else:
            try:
                mark = mark_field_locations[-3]
            except:
                mark = ''
            try:
                field = mark_field_locations[-2]
            except:
                field = ''
            try:
                location = mark_field_locations[-1]
            except:
                location = ''

        # get num of comment
        num_of_comment = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR,
            "a.js-analytics-section.js-load-project-comments.js-load-project-content.mx3.project-nav__link--comments.tabbed-nav__link.type-14 > span.count"
        ))))
        res = {
            "title": title,
            "pledged": pledged,
            "goal": goal,
            "num_of_backer": num_of_backer,
            "days_to_go": days_to_go,
            "mark": mark,
            "field": field,
            "location": location,
            "num_of_comment": num_of_comment
        }
        save_to_file(file_addr="./data/result.txt", content=res)
        res["url"] = url
        res["description"] = description
        res["picture"] = picture
        save_to_file(file_addr="./data/detail_result.txt", content=res)
        if (len(producer) == 2):
            kafka_broker, topic = producer
            print("[*] Sending to broker:", kafka_broker, ", topic:", topic)
            print(str(res))
            projectProducer = ProjectProducer(broker=kafka_broker, topic=topic)
            projectProducer.send_msg(res)

        else:

            # save to mongodb
            print("[*] Save data to mongodb.")
            print(mark_field_locations)
    except Exception as e:
        load_dotenv()
        err_url = {
            "page": page,
            "url": url,
            "err": str(e),
            # "time": datetime.now()
        }
        save_to_file(error_url_file, err_url)
        # save_to_mongodb(err_url)
        print("error in", url)
        traceback.print_exc()
    browser.close()


def get_data(url, current_page, num_of_thread, error_url_file, checkpoint_file, producer=[], web_driver_wait=5, delay_time=0.5):
    """
    This func is used to crawl data from Kickstarter website (
        https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page=
    )

    Args:
        url (string): link to website
        current_page (int): current page index of page that is being crawled
        num_of_thread (int): num of crawling thread
        error_url_file (string): name of file that contains a list of error_url_file that can not be crawled
        checkpoint_file (string): name of file that contains information about the index of page that is being crawled
    """
    page = current_page
    print("page:", page)
    while (1):
        browser = set_up_browser()
        meta_url = url+str(page)
        # print("meta_url: ")
        # print(meta_url)
        try:
            browser.get(meta_url)
            sleep(2)
            links = list(map(lambda a: a.get_attribute("href"),
                             browser.find_elements(By.CSS_SELECTOR,
                                                   "a[class='block img-placeholder w100p']")
                             ))
            prj_links = [l for l in links if l.endswith("?ref=discovery")]
            # print("prj_links: ")
            # for l in prj_links:
            #     print(l)
            # print(len(prj_links))
            threads = []
            split_prj_links = split_list(prj_links, num_of_thread)
            for links in split_prj_links:
                threads = []
                for link in links:
                    delay = random.randint(1, 15)
                    threads.append(threading.Thread(
                        target=get_detail_project, args=(current_page, link, error_url_file, producer, web_driver_wait, delay)))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                threads = []
            threads = []
            page = page+1
            file = open(checkpoint_file, "w")
            file.write(str({"page": page}))
        except:
            print("err, page = ", page)
            traceback.print_exc()
            browser.close()
            sleep(10)
            get_data(url, current_page, num_of_thread, error_url_file,
                     checkpoint_file, producer=producer, web_driver_wait=web_driver_wait, delay_time=2)
            break
        browser.close()
