from fastapi import FastAPI
from selenium.webdriver.chrome.options import Options
from utils import get_data
from utils import crawl_kickstarter_project_data
from dotenv import load_dotenv
import os
import uvicorn
load_dotenv()
url = 'https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page='

# get the current page
checkpoint = eval(open('./data/checkpoint.csv', "r").readline())
current_page = checkpoint["page"]
error_url_file = './data/error_url.csv'
print(current_page)
chrome_options = Options()
chrome_options.add_experimental_option('detach', True)
kafka_broker = os.environ.get("kafka_broker")
topic = "kickstarter-project"
# get_data(current_page=current_page, url=url, num_of_thread=4,
#          checkpoint_file="./data/checkpoint.csv", error_url_file="./data/error_url.csv",
#          producer=[kafka_broker, topic], web_driver_wait=10, delay_time=2)
crawl_kickstarter_project_data(
    current_page=current_page, producer=[kafka_broker, topic])
