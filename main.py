from fastapi import FastAPI
from selenium.webdriver.chrome.options import Options
from utils import get_data
app = FastAPI()
url = 'https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page='

# get the current page
checkpoint = eval(open('./data/checkpoint.csv', "r").readline())
current_page = checkpoint["page"]
error_url_file = 'error_url.csv'
print(current_page)
chrome_options = Options()
chrome_options.add_experimental_option('detach', True)
get_data(current_page=current_page, url=url, num_of_thread=1,
         checkpoint_file="./data/checkpoint.csv", error_url_file="./data/error_url.csv", producer=["localhost:9092", "project"])


@app.get("/")
def read_root():
    return ("Hello World.")


@app.get("/projects")
def get_data_from_kickstarter(num_of_thread: int = 4):
    get_data(current_page=current_page, url=url, num_of_thread=num_of_thread,
             checkpoint_file="./data/checkpoint.csv", error_url_file="./data/error_url.csv", producer=["localhost:9092", "project"])
