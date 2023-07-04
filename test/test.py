# test beautifulsoup
# from bs4 import BeautifulSoup
# html_content = '''
# <div>
#     <h2 class="buffa">Hello</h2>
#     <div class="h1">
#         <h2 class="geats">Dark Geats</h2>
#     </div>
# </div>
# '''

# # Phân tích mã nguồn HTML bằng BeautifulSoup
# soup = BeautifulSoup(html_content, 'html.parser')

# # Sử dụng CSS selector để lấy nội dung của phần tử h2 có class là "geats"
# h2_text = soup.select_one('h2.buffa').text

# # In ra nội dung
# print(h2_text)

# test urlib
import requests
import math
from bs4 import BeautifulSoup
s = 21111
round_num = s - s % (10**(len(str(s))-1))
print(round_num)

# test requests to indiegogo
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}
res = requests.get(
    "https://www.indiegogo.com/projects/shi-senryaku-omnibus-edition", headers=headers)
if (res.status_code == 200):
    print(res.text)
    soup = BeautifulSoup(res.text, "html.parser")
else:
    print(res.status_code)
