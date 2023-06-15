from time import sleep
import schedule

import selenium.common.exceptions
import urllib3.exceptions
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from const import *
from selenium.webdriver.chrome.options import Options

from producer import MovieProducer

broker = 'localhost:9092'
topic = 'film'
producer = MovieProducer(broker, topic)

chrome_options = Options()
chrome_options.add_experimental_option("detach", True)
client = MongoClient('localhost', 27017)
database = client['admin']
collection = database['films']


def get_data(i, serial):
    # assign values to page and film_serial for continue crawling when programme has errors
    page = i
    film_serial = serial
    # 1.Open link
    browser = webdriver.Chrome(
        service=(Service(ChromeDriverManager().install())))
    meta_link = link + str(i)
    browser.get(meta_link)
    # browser.maximize_window()
    sleep(5)
    # 2.Click to see detail information
    href = list(map(lambda a: a.get_attribute("href"),
                browser.find_elements(By.CSS_SELECTOR, 'a[class="title"]')))
    for index in range(serial, len(href)):
        film_serial = index
        l = href[index]
        print(str(page)+" "+str(film_serial)+" "+l)
        try:
            browser.get(l)
            sleep(5)
            # try:
            image = browser.find_element(
                By.CSS_SELECTOR, image_css).get_attribute("src")
            # except:
            #     image = ""
            try:
                rating_list = browser.find_elements(
                    By.CSS_SELECTOR, rating_list_css)
            except selenium.common.exceptions.NoSuchElementException:
                rating_list = [-1, -1, -1, -1, -1, -1]
            try:
                year = int(convert(browser.find_element(
                    By.CSS_SELECTOR, year_css).text))
            except:
                year = -1
            try:
                title = browser.find_element(
                    By.CSS_SELECTOR, title_css).find_element(By.TAG_NAME, 'h1').text
            except selenium.common.exceptions.NoSuchElementException:
                title = ""
            try:
                num_of_critic = int(
                    convert(rating_list[0].text.replace('based on ', '').replace(' Critic Reviews', '')))
            except:
                num_of_critic = -1
            try:
                distributor = browser.find_element(By.CSS_SELECTOR, distributor_css).find_element(By.TAG_NAME,
                                                                                                  'a').text
            except selenium.common.exceptions.NoSuchElementException:
                distributor = ""
            try:
                release_date = browser.find_element(By.CSS_SELECTOR, release_date_css).text.replace('Release Date:',
                                                                                                    '')
            except selenium.common.exceptions.NoSuchElementException:
                release_date = ""
            try:
                num_of_critic_by_level = list(
                    map(lambda a: int(convert(a.text)),
                        browser.find_elements(By.CSS_SELECTOR, num_of_critic_by_level_css)))
            except:
                num_of_critic_by_level = [-1, -1, -1, -1, -1, -1, -1]
            critic_score = -1.0
            for x in critic_score_css:
                critic_scores = browser.find_elements(By.CSS_SELECTOR, x)
                if (critic_scores != []):
                    critic_score = float(critic_scores[0].text)
                    break
            try:
                num_of_rating = int(convert(rating_list[1].text.replace(
                    'based on ', '').replace(' Ratings', '')))
            except:
                num_of_rating = -1
            rating_score = -1.0
            for x in rating_score_css:
                rating_scores = browser.find_elements(By.CSS_SELECTOR, x)
                if (rating_scores != []):
                    rating_score = float(rating_scores[0].text)
                    break
            try:
                stars = list(
                    map(lambda a: a.text,
                        browser.find_element(By.CSS_SELECTOR, stars_css).find_elements(By.TAG_NAME, 'a')))
            except selenium.common.exceptions.NoSuchElementException:
                stars = []
            try:
                facts = list(map(lambda a: a.text,
                                 browser.find_element(By.CSS_SELECTOR, facts_css).find_elements(By.TAG_NAME, 'li')))
            except selenium.common.exceptions.NoSuchElementException:
                facts = []
            try:
                summary = browser.find_element(
                    By.CSS_SELECTOR, summary_css).text.replace('Summary: ', '')
            except selenium.common.exceptions.NoSuchElementException:
                summary = ""
            try:
                director = browser.find_element(By.CSS_SELECTOR, director_css).find_element(By.TAG_NAME,
                                                                                            'a').find_element(
                    By.TAG_NAME, 'span').text
            except selenium.common.exceptions.NoSuchElementException:
                director = ""
            try:
                genres = browser.find_element(By.CSS_SELECTOR, genres_css).text.replace('Genre(s): ', '').split(
                    ', ')
            except selenium.common.exceptions.NoSuchElementException:
                genres = []
            try:
                rating = browser.find_element(
                    By.CSS_SELECTOR, rating_css).text.replace("Rating:", '')
            except selenium.common.exceptions.NoSuchElementException:
                rating = ""
            try:
                runtime = int(convert(
                    browser.find_element(By.CSS_SELECTOR, runtime_css).text.replace("Runtime:", '').replace(" min",
                                                                                                            '')))
            except:
                runtime = -1
            record = {
                'title': title,
                'year': year,
                'image': image,
                'distributor': distributor,
                'release_date': release_date,
                'num_of_critic': num_of_critic,
                'critic_positive': num_of_critic_by_level[0],
                'critic_mixed': num_of_critic_by_level[1],
                'critic_negative': num_of_critic_by_level[2],
                'critic_score': critic_score,
                'num_of_rating': num_of_rating,
                'rating_positive': num_of_critic_by_level[3],
                'rating_mixed': num_of_critic_by_level[4],
                'rating_negative': num_of_critic_by_level[5],
                'rating_score': rating_score,
                'stars': stars,
                'facts': facts,
                'summary': summary,
                'director': director,
                'genres': genres,
                'rating': rating,
                'runtime': runtime
            }
            # collection.insert_one(record)
            producer.send_msg(record)
            file = open("data.csv", "w")
            file.write(str({"film_serial": film_serial, "page": page}))
            file.close()
        except selenium.common.exceptions.NoSuchElementException:
            print("No such element exception.")
        except selenium.common.exceptions.TimeoutException:
            browser.close()
            get_data(page, film_serial)
            break
        except urllib3.exceptions.MaxRetryError:
            browser.close()
            get_data(page, film_serial)
            break
        except:
            browser.close()
            get_data(page, film_serial)
            break
    # close browser
    browser.close()


# open link
def run():
    print('[*] Start running producer to get data')
    filmObj = eval(open("data.csv", "r").readline())
    film_serial = filmObj["film_serial"]
    page = filmObj["page"]
    if (film_serial == 99):
        page += 1
        film_serial = 0
    else:
        film_serial += 1
    get_data(page, film_serial)
    for i in range(page+1, 152):
        get_data(i, 0)

# schedule producer
# schedule.every(1).minutes.do(run)
# while True:
#     schedule.run_pending()
#     sleep(1)


run()
