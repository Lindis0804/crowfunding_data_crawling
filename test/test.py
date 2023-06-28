# from n_kafka.producer import ProjectProducer
# import traceback
# broker, topic = "localhost:9092", "kickstarter-project"
# print("[*] Sending to broker "+broker+", topic: "+topic)
# msg = "Oh my gosh."
# print(msg)
# projectProducer = ProjectProducer(broker=broker, topic=topic)
# try:
#     projectProducer.send_msg(msg)
#     print("[*] Send to kafka successfully.")
# except:
#     print("[*] Send to kafka fail.")
#     traceback.print_exc()
# import json
# import os
# from dotenv import load_dotenv
# import undetected_chromedriver as uc
# from time import sleep
# from project.
# load_dotenv()
# obj = {
#     "name": "Le Dinh Hieu",
#     "MSSV": "20194280"
# }
# s = json.dumps(obj).encode('utf-8')
# print(s)
# s1 = json.loads(s.decode('utf-8'))
# print(s1)
# print(obj["name"])
# browser = uc.Chrome()
# browser.get(
#     "https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page=0")
# sleep(10)
file = open("./data/err_url.csv", "a")
file.write("hello\n")
file.write("hi")
# ls = ['', 1, 2, 3, '', '']
# le = len(ls)
# for i in range(le-1, -1, -1):
#     if (ls[i] == ''):
#         ls.pop(i)
#     else:
#         break
# print(ls)
obj = {'id': 990203322, 'name': 'iflow: The Smart Bottle for Peak Performance and Wellness',
       'blurb': 'Experience a Transformative Journey with an Advanced Smart Bottle | As Seen on 2 Minuten 2 Millionen!',
       'goal': 4500.0, 'pledged': 1731.0, 'state': 'live', 'slug': 'i-flow-the-smart-bottle-for-peak-performance-and-wellness',
       'disable_communication': False, 'country': 'CH', 'country_displayable_name': 'Switzerland', 'currency': 'CHF', 'currency_symbol': 'Fr ',
       'currency_trailing_code': False, 'deadline': 1689199140, 'state_changed_at': 1686675978, 'created_at': 1683897695, 'launched_at': 1686675976,
       'staff_pick': False, 'is_starrable': True, 'backers_count': 13, 'static_usd_rate': 1.10623752, 'usd_pledged': '1914.89714712', 'converted_pledged_amount': 1936,
       'fx_rate': 1.11850943, 'usd_exchange_rate': 1.11850943, 'current_currency': 'USD', 'usd_type': 'international',
       'creator': {'id': 58693289, 'name': 'Dmitry | Founder of I-Flow', 'slug': 'i-flow', 'is_registered': None, 'is_email_verified': None, 'chosen_currency': None,
                   'is_superbacker': None, 'urls': {'web': {'user': 'https://www.kickstarter.com/profile/i-flow'},
                                                    'api': {'user': 'https://api.kickstarter.com/v1/users/58693289?signature=1688015398.3d7f0d7bc8b638c2d2c3df6da2eb25e3d0b572e5'}}},
       'location': {'id': 784794, 'name': 'Zurich', 'slug': 'zurich-zurich-canton-of-zurich', 'short_name': 'Zurich, Switzerland', 'displayable_name': 'Zurich, Switzerland',
                    'localized_name': 'Zurich', 'country': 'CH', 'state': 'Canton of Zurich', 'type': 'Town', 'is_root': False, 'expanded_country': 'Switzerland',
                    'urls': {'web': {'discover': 'https://www.kickstarter.com/discover/places/zurich-zurich-canton-of-zurich',
                                     'location': 'https://www.kickstarter.com/locations/zurich-zurich-canton-of-zurich'},
                             'api': {'nearby_projects': 'https://api.kickstarter.com/v1/discover?signature=1687973003.0765542806b7d8db414986dee62d6351f06a50fa&woe_id=784794'}}},
       'category': {'id': 337, 'name': 'Gadgets', 'analytics_name': 'Gadgets', 'slug': 'technology/gadgets', 'position': 7, 'parent_id': 16, 'parent_name': 'Technology', 'color': 6526716,
                    'urls': {'web': {'discover': 'http://www.kickstarter.com/discover/categories/technology/gadgets'}}},
       'profile': {'id': 4616972, 'project_id': 4616972, 'state': 'inactive', 'state_changed_at': 1683897695, 'name': None, 'blurb': None, 'background_color': None,
                   'text_color': None, 'link_background_color': None, 'link_text_color': None, 'link_text': None, 'link_url': None, 'show_feature_image': False,
                   'background_image_opacity': 0.8, 'should_show_feature_image_section': True,
                   'feature_image_attributes': {'image_urls': {'default': 'https://ksr-ugc.imgix.net/assets/041/281/800/c977b8c4c5426af4a7a870f8901fc12b_original.png?ixlib=rb-4.0.2&crop=faces&w=1552&h=873&fit=crop&v=1686671123&auto=format&frame=1&q=92&s=84e79d465c4442e13bdcc74d5ae9eb93', 'baseball_card': 'https://ksr-ugc.imgix.net/assets/041/281/800/c977b8c4c5426af4a7a870f8901fc12b_original.png?ixlib=rb-4.0.2&crop=faces&w=560&h=315&fit=crop&v=1686671123&auto=format&frame=1&q=92&s=c1ae1b58b061df429e49a2df5d770330'}}},
       'spotlight': False, 'percent_funded': 38.46666666666667, 'is_liked': False, 'is_disliked': False}
del obj["creator"]["urls"]
del obj["category"]["urls"]
del obj["location"]["urls"]
del obj["profile"]
del_list = ["state", "disable_communication", "currency_trailing_code",
            'state_changed_at', 'staff_pick', 'is_starrable']
for l in del_list:
    if l in obj:
        del obj[l]
print(obj)
