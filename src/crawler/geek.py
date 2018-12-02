# -*- encoding:utf-8 -*-

import time
import random

# 将网页生成pdf并保存到本地
import pyautogui as pag


mouse_move = [pag.easeInBounce,pag.easeInElastic,pag.easeInOutQuad,pag.easeInOutSine]

total_web_count = 15
current_web_count = 6

time.sleep(3)
# print pag.position()

while current_web_count < total_web_count:
    current_web_count = current_web_count + 1
    time.sleep(random.uniform(0.3,1.3))
    title_x = random.uniform(407,786)
    title_y = random.uniform(200,233)
    pag.moveTo(title_x,title_y,duration=random.uniform(0.1,0.9),tween=mouse_move[random.randint(0,len(mouse_move)-1)])
    time.sleep(random.uniform(0.3,1.3))
    pag.tripleClick(title_x,title_y,interval=random.uniform(0.3,1.1),duration=random.uniform(0.1,0.9),
                    tween=mouse_move[random.randint(0,len(mouse_move)-1)])
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('command', 'c')
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('command','p') # Command + p 在chrome中表示打印
    time.sleep(random.uniform(5,7))
    pag.hotkey('enter')     # 点击保存
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('command','v')
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('enter')  # 确定打印
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('command','f')  # 查找
    # time.sleep(random.uniform(0.3,1.3))
    # pag.typewrite('下一篇',random.uniform(0.8,1.2))  # 输入查找内容，这里注释掉，需提前把这一步做好
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('enter')  # 回车查找
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('command','f')  # 查找 执行两遍，发现有时候会失效，所以执行两遍
    # time.sleep(random.uniform(0.3,1.3))
    # pag.typewrite('下一篇',random.uniform(0.8,1.2))  # 输入查找内容，这里注释掉，需提前把这一步做好
    time.sleep(random.uniform(0.3,1.3))
    pag.hotkey('enter')  # 回车查找
    time.sleep(random.uniform(0.3,1.3))
    next_x = random.uniform(420,720)
    next_y = random.uniform(554,571)
    pag.moveTo(next_x,next_y,duration=random.uniform(0.1,0.9),tween=mouse_move[random.randint(0,len(mouse_move)-1)])
    time.sleep(random.uniform(0.3,1.3))
    pag.click(next_x,next_y,interval=random.uniform(0.3,1.1),duration=random.uniform(0.1,0.9),
                    tween=mouse_move[random.randint(0,len(mouse_move)-1)])
    time.sleep(random.uniform(0.3,1.3))

# title_x = random.uniform(407,786)
# title_y = random.uniform(200,233)
# pag.moveTo(title_x,title_y,duration=random.uniform(0.6,1.6))
# 三连击
# pag.hotkey('command','c')
# time.sleep(random.uniform(0.3,1.3))
#
# pag.hotkey('command','p') # Command + p 在chrome中表示打印
# time.sleep(random.uniform(5,7))
# pag.hotkey('enter')     # 点击保存
# time.sleep(random.uniform(0.3,1.3))
# pag.hotkey('command','v')
# time.sleep(random.uniform(0.3,1.3))
# pag.hotkey('enter')  # 确定打印
# time.sleep(random.uniform(0.3,1.3))
# pag.hotkey('command','f')  # 查找
# time.sleep(random.uniform(0.3,1.3))
# pag.typewrite('下一篇',random.uniform(0.8,1.2))  # 查找
# time.sleep(random.uniform(0.3,1.3))
# pag.hotkey('enter')  # 确定打印
# next_x = random.uniform(407,786)
# next_y = random.uniform(200,233)
# pag.hotkey('command','c')
# time.sleep(random.uniform(0.3,1.3))




# while current_web_count < total_web_count:
#     pag.hotkey('command','p') # Command + p 在chrome中表示打印
#     time.sleep(2)
#     pag.hotkey('enter')     # 点击保存
#     time.sleep(1)
#     pag.typewrite('Hello world!', 0.15)  # 修改文件名称
#     time.sleep(1)
#     pag.hotkey('enter')  # 确定打印
#
#
#     time.sleep(2)

