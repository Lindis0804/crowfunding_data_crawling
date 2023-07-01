from selenium.webdriver.chrome.options import Options
from utils import get_data
from utils import crawl_kickstarter_project_data
from utils import crawl_diegogo_project_data
from dotenv import load_dotenv
import os
import threading
import json
load_dotenv()
url = 'https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page='

# get the current page
with open("./data/indiegogo_checkpoint.json", "r") as jf:
    indiegogo_cur_page = json.load(jf)["page"]
with open("./data/kickstarter_checkpoint.json", "r") as jf:
    kickstarter_cur_page = json.load(jf)["page"]
print(indiegogo_cur_page, kickstarter_cur_page)
chrome_options = Options()
chrome_options.add_experimental_option('detach', True)
kafka_broker = os.environ.get("kafka_broker")
k_topic = os.environ.get("kickstarter-project")
i_topic = os.environ.get("indiegogo-project")
topic = "project"
# get_data(current_page=current_page, url=url, num_of_thread=4,
#          checkpoint_file="./data/checkpoint.csv", error_url_file="./data/error_url.csv",
#          producer=[kafka_broker, topic], web_driver_wait=10, delay_time=2)
# kafka_server = [kafka_broker, topic]
kafka_server = []
i_thread = threading.Thread(target=crawl_diegogo_project_data, args=(
    indiegogo_cur_page, kafka_server))
k_thread = threading.Thread(target=crawl_kickstarter_project_data, args=(
    kickstarter_cur_page, kafka_server))
i_thread.start()
k_thread.start()
i_thread.join()
k_thread.join()
