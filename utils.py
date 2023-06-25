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
from fastapi import FastAPI
from n_kafka.producer import ProjectProducer
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime


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
    for l in lsts:
        if (len(l) != 0):
            return l
    return lsts[-1]


def get_detail_project(page, url, error_url_file, producer=[], web_driver_wait=5, delay_time=0.5):
    """
    This func is used to crawl data of a detail project page of Kickstarter website

    Args:
        page (int): page index of url
        url (string): link to website that need to be crawled
        error_url_file (string): name of file that contains a list of error urls that can not be crawled (
            this list will be executed later
        )
    """
    browser = webdriver.Chrome(
        service=(Service(ChromeDriverManager().install()))
    )
    sleep(delay_time)
    try:
        print("crawl url: ")
        print(url)
        browser.get(url)
        sleep(2)
        # t = title.text
        wait = WebDriverWait(browser, web_driver_wait)
        title = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "h2.type-24-md.soft-black.mb1.project-name"))))
        description = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "p[class='type-14 type-18-md soft-black project-description mb1']"))))
        picture = browser.find_element(
            By.CSS_SELECTOR, "img[class='aspect-ratio--object bg-black z3']").get_attribute("src")
        pledged = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "span[class='ksr-green-500']"))))
        span_goal = browser.find_elements(
            By.CSS_SELECTOR, "span[class='inline-block-sm hide']")
        n_goal = []
        for i in span_goal:
            goals = i.find_elements(By.CSS_SELECTOR, "span[class='money']")
            for goal in goals:
                n_goal.append(goal.text)
        goal = get_first_not_null_item(n_goal)
        num_of_backer = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "div[class='block type-16 type-28-md bold dark-grey-500']"))))
        days_to_go = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "span[class='block type-16 type-28-md bold dark-grey-500']"))))
        mark_field_locations = list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR, "a.nowrap.navy-700.flex.items-center.medium.mr3.type-12.keyboard-focusable > span.ml1"
        )))
        mark = mark_field_locations[-3]
        field = mark_field_locations[-2]
        location = mark_field_locations[-1]
        num_of_comment = get_first_not_null_item(list(map(lambda a: a.text, browser.find_elements(
            By.CSS_SELECTOR,
            "a.js-analytics-section.js-load-project-comments.js-load-project-content.mx3.project-nav__link--comments.tabbed-nav__link.type-14 > span.count"
        ))))
        res = {
            "title": title,
            "description": description,
            "picture": picture,
            "pledged": pledged,
            "goal": goal,
            "num_of_backer": num_of_backer,
            "days_to_go": days_to_go,
            "mark": mark,
            "field": field,
            "location": location,
            "num_of_comment": num_of_comment
        }
        if (len(producer) == 2):
            kafka_broker, topic = producer
            print("[*] Sending to broker:", kafka_broker, ", topic:", topic)
            print(str(res))
            projectProducer = ProjectProducer(broker=kafka_broker, topic=topic)
            projectProducer.send_msg(res)
            print("[*] Send to kafka successfully.")

        else:
            # file = open("./data/result.txt", "a")
            # file.write(url+"\n")
            # file.write(
            #     str([title, description, picture, pledged, goal, backers, days_to_go])+"\n")
            # file.close()

            # save to mongodb
            print("[*] Save data to mongodb.")
    except Exception as e:
        # load_dotenv()
        # err_url = {
        #     "page": page,
        #     "url": url,
        #     "err": str(e),
        #     "time": datetime.now()
        # }
        # try:
        #     client = MongoClient(os.environ.get("mongodb"))
        #     db = client["local"]
        #     collection = db["kickstarter_err_url"]
        #     result = collection.insert_one(err_url)
        #     client.close()
        # except:
        #     traceback.print_exc()
        print("error in", url)
        # print("error")
        # traceback.print_exc()
    browser.close()
# get_data(current_page=current_page)
# get_detail_project(0,"https://www.kickstarter.com/projects/mlspencer/dragon-mage-deluxe-collectors-edition-hardcover")


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
    while (1):
        browser = webdriver.Chrome(
            service=(Service(ChromeDriverManager().install()))
        )
        meta_url = url+str(page)
        print("meta_url: ")
        print(meta_url)
        try:
            browser.get(meta_url)
            sleep(2)
            links = list(map(lambda a: a.get_attribute("href"),
                             browser.find_elements(By.CSS_SELECTOR,
                                                   "a[class='block img-placeholder w100p']")
                             ))
            prj_links = [l for l in links if l.endswith("?ref=discovery")]
            print("prj_links: ")
            for l in prj_links:
                print(l)
            print(len(prj_links))
            threads = []
            split_prj_links = split_list(prj_links, num_of_thread)
            for links in split_prj_links:
                threads = []
                delay = 0
                for link in links:
                    delay = delay+delay_time
                    threads.append(threading.Thread(
                        target=get_detail_project, args=(current_page, link, error_url_file, producer, web_driver_wait, delay)))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                threads = []
            threads = []
            page = page+1
        except:
            print("err, page = ", page)
            file = open(checkpoint_file, "w")
            file.write(str({"page": page}))
            traceback.print_exc()
            browser.close()
            get_data(url, current_page, num_of_thread, error_url_file,
                     checkpoint_file, producer=producer, web_driver_wait=web_driver_wait, delay_time=2)
            break
        browser.close()
