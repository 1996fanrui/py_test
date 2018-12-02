# -*- encoding:utf-8 -*-


from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import time

# 将网页生成pdf并保存到本地
import pyautogui as pag

url = 'http://www.tit.edu.cn/'
driver = webdriver.Chrome()
driver.maximize_window()
driver.fullscreen_window()
driver.get(url)


cookies = driver.get_cookies()

for cookie in cookies:
    driver.add_cookie(cookie)

pag.hotkey('command','p') # Command + p 在chrome中表示打印
time.sleep(2)
pag.hotkey('enter')     # 点击保存
time.sleep(1)
pag.typewrite('Hello world!', 0.15)  # 修改文件名称
time.sleep(1)
pag.hotkey('enter')  # 确定打印


time.sleep(2)

driver.quit()

