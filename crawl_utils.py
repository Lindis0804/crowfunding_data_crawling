from datetime import datetime
import pytz
import json
import requests
from n_kafka.producer import ProjectProducer
from utils import save_to_json_file
import traceback
from time import sleep
from utils import set_up_browser
from utils import round_number
from utils import split_list
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import threading


def send_data(data=[], producer=[]):
    if (len(producer) == 2):
        kafka_broker, topic = producer
        print("[*] Data from kickstarter sending to broker:",
              kafka_broker, ", topic:", topic)
        # print(f"data: {data}")
        projectProducer = ProjectProducer(broker=kafka_broker, topic=topic)
        for item in data:
            # item["timestamp"] = get_present()
            projectProducer.send_msg(item)
    else:
        # save to mongodb
        # print("[*] Save data to mongodb.")
        print(data)


def get_present():
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')


def convert_timestring_to_unix(time_string):

    # convert time string to datetime object
    dt = datetime.fromisoformat(time_string)

    # define timezone
    timezone = pytz.timezone("America/Los_Angeles")

    # apply timezone for datetime
    localized_dt = timezone.localize(dt.replace(tzinfo=None))

    # convert datetime object to unix timestamp
    unix_time = int(localized_dt.timestamp())

    return unix_time


def get_and_format_indiegogo_project_data(input, producer=[], web_driver_wait=5):
    data = input

    # change category
    del data["category_url"]
    data["category"] = [{"name": data.pop("category")}]

    # change close date -> deadline
    data["deadline"] = convert_timestring_to_unix(data.pop("close_date"))

    # change open date -> launched_at
    data["launched_at"] = convert_timestring_to_unix(data.pop("open_date"))

    # change fund_raised_amount -> pledged
    data["pledged"] = data.pop("funds_raised_amount")

    # change funds_raised_percent -> percent_funded
    data["percent_funded"] = data.pop("funds_raised_percent")

    # change title -> name
    data["name"] = data.pop("title")

    # change tagline -> blurb
    data["blurb"] = data.pop("tagline")

    if (data["pledged"] == 0 or data["percent_funded"] == 0):
        data["goal"] = 0
    else:
        data["goal"] = round_number(
            int(data["pledged"]/data["percent_funded"]))

    # open browser to get backers_count,location, goal
    browser = set_up_browser()
    try:
        browser.get("https://www.indiegogo.com"+data["clickthrough_url"])
        print(f"url: {'https: // www.indiegogo.com'+data['clickthrough_url']}")
        wait = WebDriverWait(browser, web_driver_wait)
        # browser.get_screenshot_as_file("screenshot.png")
        backer_elements = browser.find_elements(
            By.CSS_SELECTOR, "span.basicsGoalProgress-claimedOrBackers > span")
        # print(f"backers_elements: {backer_elements}")
        try:
            backers_count = backer_elements[-2].text
        except:
            backers_count = 0
        country_elements = browser.find_elements(
            By.CSS_SELECTOR, "div.basicsCampaignOwner-details-city"
        )
        try:
            country = country_elements[0].text.split(", ")[-1]
        except:
            country = ""
        data["backers_count"] = backers_count
        data["country"] = country
        data["timestamp"] = get_present()
        data["web"] = "indiegogo"
        send_data([data], producer=producer)
    except:
        traceback.print_exc()
        # change project_type -> ????

        # change tags to -> ?????
    return data


def crawl_diegogo_project_data(current_page, producer=[],
                               web_driver_wait=2,
                               num_of_thread=5,
                               url="https://www.indiegogo.com/private_api/discover",
                               err_page_file="./data/indiegogo_err_page.json",
                               check_point_file="./data/indiegogo_checkpoint.json",
                               sleep_per_crawling_time=60,
                               ):
    page = current_page
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    while (1):
        try:
            request_body = json.dumps({"sort": "trending", "category_main": None, "category_top_level": None,
                                       "project_timing": "all", "project_type": "campaign", "page_num": page, "per_page": 100, "q": "", "tags": []})
            res = requests.post(url, data=request_body, headers=headers)
            if res.status_code == 200:
                data = json.loads(res.text)["response"]["discoverables"]
                split_data = split_list(data, num_of_thread)
                for dts in split_data:
                    threads = []
                    for dt in dts:
                        threads.append(threading.Thread(
                            target=get_and_format_indiegogo_project_data, args=(dt, producer, web_driver_wait)))
                    for thr in threads:
                        thr.start()
                    for thr in threads:
                        thr.join()
            # save_to_json_file("./data/indiegogo_data.json", data)
            elif res.status_code == 400:
                page = -1
                sleep(sleep_per_crawling_time)
        except Exception as e:
            err_page = {
                "page": page,
                "err": str(e),
            }
            save_to_json_file(err_page_file, err_page)

            traceback.print_exc()
        page = page+1
        save_to_json_file(check_point_file, {"page": page})


def field_filter(x):
    obj = json.loads(x)
    del obj["creator"]["avatar"]
    del obj["creator"]["urls"]
    del obj["category"]["urls"]
    del obj["location"]["urls"]
    del_list = ["state", "disable_communication", "currency_trailing_code",
                'state_changed_at', 'staff_pick', 'is_starrable', "photo", "urls", "profile"]
    for l in del_list:
        if l in obj:
            del obj[l]
    obj["category"] = [obj.pop("category")]
    obj["timestamp"] = get_present()
    obj["web"] = "kickstarter"
    return obj


def get_kickstarter_project_data_list_by_page(page,
                                              url="https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page=",
                                              error_page_file="./data/kickstarter_err_page.json", producer=[], web_driver_wait=5, delay_time=0.5):
    browser = set_up_browser()
    sleep(delay_time)
    try:
        browser.get(url+str(page))
        wait = WebDriverWait(browser, web_driver_wait)
        browser.get_screenshot_as_file("screenshot.png")
        elements = browser.find_elements(
            By.CSS_SELECTOR, "div[class='js-react-proj-card grid-col-12 grid-col-6-sm grid-col-4-lg']")
        data = list(map(lambda x: field_filter(
            x.get_attribute("data-project")), elements[6:]))
        send_data(data, producer=producer)
    except Exception as e:
        err_page = {
            "page": page,
            "err": str(e),
        }
        save_to_json_file(error_page_file, err_page)
        traceback.print_exc()
    browser.close()
    # get_detail_project(
    #     0, "https://www.kickstarter.com/projects/alexispowell/stay-at-home-to-sleep-in-lonely-gimmicks-ep?ref=discovery")


def crawl_kickstarter_project_data(current_page, producer=[],
                                   url="https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page=",
                                   error_page_file="./data/kickstarter_err_page.json",
                                   check_point_file="./data/kickstarter_checkpoint.json",
                                   web_driver_wait=10, delay_time=5, sleep_per_crawling_time=60):
    page = current_page
    while (1):
        get_kickstarter_project_data_list_by_page(
            page, url, error_page_file, producer, web_driver_wait, delay_time)
        if page <= 49268:
            page = page+1
        else:
            page = 0
            sleep(sleep_per_crawling_time)
        save_to_json_file(check_point_file, {"page": page})


def format_crowdfunder_project_data(input):
    data = {
        "id": input["idproject"],
        "name": input["title"],
        "goal": input["amount"],
        "pledged": input["amount_pledged"],
        "percent_funded": input["percent_funded"],
        "country": input["country"],
        "created_at": convert_timestring_to_unix(input["created_at"]),
        "launched_at": convert_timestring_to_unix(input["created_at"]),
        "deadline": convert_timestring_to_unix(input["closing_at"]),
        "backers_count": input["num_backers"],
        "category": [{"name": cate} for cate in input["category"]],
        "timestamp": get_present(),
        "web": "crowdfunder"
    }
    return data


def crawl_crowdfunder_project_data(current_page, producer=[],
                                   url="https://7izdzrqwm2-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.17.0)%3B%20Browser%20(lite)&x-algolia-api-key=9767ce6d672cff99e513892e0b798ae2&x-algolia-application-id=7IZDZRQWM2&paginationLimitedTo=2000",
                                   err_page_file="./data/crowdfunder_err_page.json",
                                   check_point_file="./data/crowdfunder_checkpoint.json",
                                   sleep_per_crawling_time=60
                                   ):
    page = current_page
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    while (1):
        for i in range(page, 50):
            try:
                request_body = json.dumps({"requests": [
                                          {"indexName": "frontendsearch", "params": "aroundPrecision=1000&distinct=true&facetFilters=%5B%5B%5D%5D&facets=%5B%5D&hitsPerPage=20&insideBoundingBox=&page="+str(i)+"&query=&tagFilters="}]})
                res = requests.post(url, data=request_body, headers=headers)
                if (res.status_code == 200):
                    data = list(map(lambda x: format_crowdfunder_project_data(
                        x), json.loads(res.text)["results"][0]["hits"]))
                    send_data(data, producer)
                else:
                    print(f"status_code: {res.status_code}")
                    raise Exception("Error.")
            except Exception as e:
                err_page = {
                    "page": i,
                    "err": str(e),
                }
                save_to_json_file(err_page_file, err_page)
                traceback.print_exc()
                break
            save_to_json_file(check_point_file, {"page": i})
        page = 0
        sleep(sleep_per_crawling_time)
