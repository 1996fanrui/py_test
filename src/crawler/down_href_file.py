# -*- encoding:utf-8 -*-

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import time

# 从网站的超链接中下载文件
url = 'http://jwc.tit.edu.cn/info/1106/3626.htm'
driver = webdriver.Chrome()
driver.maximize_window()
driver.fullscreen_window()
driver.get(url)


cookies = driver.get_cookies()

for cookie in cookies:
    driver.add_cookie(cookie)

actionChains = ActionChains(driver)

# < script language = "javascript" src = "/system/resource/js/ajax.js" > < / script >
# < span > 附件【 < a href = "/system/_content/download.jsp?urltype=news.DownloadAttachUrl&owner=1129870260&wbfileid=2232554" >
# < span > 2019年硕考监考人员名单 - XX系.docx < / span > < / a >】 < / span >
# < span > 已下载 < span id = "nattach2232554" >
# < script language = "javascript" > getClickTimes(2232554, 1129870260, "wbnewsfile","attach") < / script > < / span > 次 < / span > < br >

# 寻找该点击的链接的位置，以上为源码
hrefs = driver.find_elements_by_link_text(u'2019年硕考监考人员名单-XX系.docx')

# click
for href in hrefs:
    href.click()

time.sleep(10)

driver.quit()

