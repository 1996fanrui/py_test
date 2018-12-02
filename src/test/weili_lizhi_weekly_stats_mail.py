# coding=utf-8
'''
    微鲤头条最近一周的新增用户行为数据统计
'''
import email
import smtplib
from email.mime.text import MIMEText

import arrow
from pyspark import HiveContext
from pyspark import SparkContext

sc = SparkContext(appName="WEILI_WEEKLY_STATS_MAIL")
sqlContext = HiveContext(sc)
yesterday = arrow.now().replace(days=-1).format('YYYYMMDD')
sevenDayAgo = arrow.now().replace(days=-7).format('YYYYMMDD')

WEILI_LIZHI_WEEKLY_STATS_QUERY = '''
select *
  FROM prod.ads_hive_dmp_weili_lizhi_weekly_stats_1w
order by week desc
limit 6
'''
weili_data = sqlContext.sql(WEILI_LIZHI_WEEKLY_STATS_QUERY)
weili_data_stats = weili_data.collect()

YOUMENG_STATS_QUERY = '''
select *
  from prod.ads_hive_weili_umeng_api_stat_1w
order by week desc
limit 6
'''
youmeng_data = sqlContext.sql(YOUMENG_STATS_QUERY)
youmeng_data_stats = youmeng_data.collect()

content = u''       # mail_content

# 友盟表A表格
def add_table_youmeng_A(data,title):
    '''
    封装数据表转化成html table逻辑
    :param data: list,elem metedata is define by header
    :param title: html表格的标题
    :return: None
    '''
    global content
    content = content + u'<b>%s</b>' %title
    table_content = []
    table_content.append(u'<br>')
    html_table_header = u'<table border = "1" cellspacing="0" bgcolor="white"><tr>'
    html_table_header += u'<th>周</th>'
    html_table_header += u'<th>DNU</th>'
    html_table_header += u'<th>新增次日留存</th>'
    html_table_header += u'<th>DAU</th>'
    html_table_header += u'<th>WAU</th>'
    html_table_header += u'<th>周活跃度</th>'
    html_table_header += u'<th>单次使用时长</th>'
    table_content.append(html_table_header)
    for row in data:
        html_row_str = u'<tr>'
        html_row_str += u'<td>%s</td>' %(row['week'])
        html_row_str += u'<td>%.0f</td>' %(row['a_dnu'])
        html_row_str += u'<td>%.2f%%</td>' %(100.0*row['a_average_retain'])
        html_row_str += u'<td>%.0f</td>' %(row['a_dau'])
        html_row_str += u'<td>%.0f</td>' %(row['a_wau'])
        html_row_str += u'<td>%.2f</td>' %(row['a_week_active'])
        html_row_str += u'<td>%02d:%02d:%02d</td>' %(row['a_average_duration']/3600,row['a_average_duration']%3600/60,row['a_average_duration']%60)
        table_content.append(html_row_str)
    table_content.append(u'</table>')
    content = content + u'\n'.join(table_content)

add_table_youmeng_A(youmeng_data_stats, u'次留低于30%渠道的基础数据')


# 友盟表B表格
def add_table_youmeng_B(data,title):
    '''
    封装数据表转化成html table逻辑
    :param data: list,elem metedata is define by header
    :param title: html表格的标题
    :return: None
    '''
    global content
    content = content + u'<br><b>%s</b>' %title
    table_content = []
    table_content.append(u'<br>')
    html_table_header = u'<table border = "1" cellspacing="0" bgcolor="white"><tr>'
    html_table_header += u'<th>周</th>'
    html_table_header += u'<th>DNU</th>'
    html_table_header += u'<th>新增次日留存</th>'
    html_table_header += u'<th>DAU</th>'
    html_table_header += u'<th>WAU</th>'
    html_table_header += u'<th>周活跃度</th>'
    html_table_header += u'<th>单次使用时长</th>'
    table_content.append(html_table_header)
    for row in data:
        html_row_str = u'<tr>'
        html_row_str += u'<td>%s</td>' %(row['week'])
        html_row_str += u'<td>%.0f</td>' %(row['dnu'])
        html_row_str += u'<td>%.2f%%</td>' %(100.0*row['average_retain'])
        html_row_str += u'<td>%.0f</td>' %(row['dau'])
        html_row_str += u'<td>%.0f</td>' %(row['wau'])
        html_row_str += u'<td>%.2f</td>' %(row['week_active'])
        html_row_str += u'<td>%02d:%02d:%02d</td>' %(row['average_duration']/3600,row['average_duration']%3600/60,row['average_duration']%60)
        table_content.append(html_row_str)
    table_content.append(u'</table>')
    content = content + u'\n'.join(table_content)

add_table_youmeng_B(youmeng_data_stats, u'次留高于30%渠道的基础数据')


# 收徒相关表格
def add_table_spread(data,title):
    '''
    封装数据表转化成html table逻辑
    :param data: list,elem metedata is define by header
    :param title: html表格的标题
    :return: None
    '''
    global content
    spread_head = ['week','invite_count','spread_count','spread_per','invite_dau','gold']
    content = content + u'<br><b>%s</b>' %title
    table_content = []
    table_content.append(u'<br>')
    html_table_header = u'<table border = "1" cellspacing="0" bgcolor="white"><tr>'
    html_table_header += u'<th>周</th>'
    html_table_header += u'<th>日均师傅数量</th>'
    html_table_header += u'<th>日均收徒数量</th>'
    html_table_header += u'<th>人均收徒数量</th>'
    html_table_header += u'<th>师傅/DAU</th>'
    html_table_header += u'<th>拉新金币花费</th>'
    table_content.append(html_table_header)

    for row in data:
        html_row_str = u'<tr>'
        html_row_str += u'<td>%s</td>' %(row['week'])
        html_row_str += u'<td>%.0f</td>' %(row['invite_count'])
        html_row_str += u'<td>%.0f</td>' %(row['spread_count'])
        html_row_str += u'<td>%.2f</td>' %(row['spread_per'])
        html_row_str += u'<td>%.2f%%</td>' %(100.0*row['invite_dau'])
        html_row_str += u'<td>%.2f</td>' %(row['gold'])
        table_content.append(html_row_str)

    table_content.append(u'</table>')
    content = content + u'\n'.join(table_content)


add_table_spread(weili_data_stats, u'微鲤头条收徒相关数据统计')


# IM相关表格
def add_table_IM(data,title):
    '''
    封装数据表转化成html table逻辑
    :param data: list,elem metedata is define by header
    :param title: html表格的标题
    :return: None
    '''
    global content
    IM_head = ['week','im_rate','rescuit_count','rescuit_per','rescuit_rate']
    content = content + u'<br><b>%s</b>' %title
    table_content = []
    table_content.append(u'<br>')
    html_table_header = u'<table border = "1" cellspacing="0" bgcolor="white"><tr>'
    html_table_header += u'<th>周</th>'
    html_table_header += u'<th>IM用户占比</th>'
    html_table_header += u'<th>招募成功数</th>'
    html_table_header += u'<th>人均招募成功数</th>'
    html_table_header += u'<th>招募成功率</th>'
    table_content.append(html_table_header)

    for row in data:
        html_row_str = u'<tr>'
        html_row_str += u'<td>%s</td>' %(row['week'])
        html_row_str += u'<td>%.2f%%</td>' %(100.0*row['im_rate'])
        if row['rescuit_count'] is None :
            html_row_str += u'<td>%s</td>' %(row['rescuit_count'])
        else :
            html_row_str += u'<td>%.0f</td>' %(row['rescuit_count'])

        if row['rescuit_per'] is None :
            html_row_str += u'<td>%s</td>' %(row['rescuit_per'])
        else :
            html_row_str += u'<td>%.2f</td>' %(row['rescuit_per'])

        if row['rescuit_rate'] is None :
            html_row_str += u'<td>%s</td>' %(row['rescuit_rate'])
        else :
            html_row_str += u'<td>%.2f%%</td>' %(100.0*row['rescuit_rate'])

        table_content.append(html_row_str)
    table_content.append(u'</table>')
    content = content + u'\n'.join(table_content)

add_table_IM(weili_data_stats, u'微鲤头条IM相关数据统计')


# 社区相关表格
def add_table_community(data,title):
    '''
    封装数据表转化成html table逻辑
    :param data: list,elem metedata is define by header
    :param title: html表格的标题
    :return: None
    '''
    global content
    content = content + u'<br><b>%s</b>' %title
    table_content = []
    table_content.append(u'<br>')
    html_table_header = u'<table border = "1" cellspacing="0" bgcolor="white"><tr>'
    html_table_header += u'<th>周</th>'
    html_table_header += u'<th>社区PV</th>'
    html_table_header += u'<th>社区UV</th>'
    html_table_header += u'<th>社区Click</th>'
    html_table_header += u'<th>社区Click UV</th>'
    html_table_header += u'<th>社区人均PV</th>'
    html_table_header += u'<th>社区CTR</th>'
    html_table_header += u'<th>社区活跃用户次日留存率</th>'
    html_table_header += u'<th>日均发帖数（图文贴）</th>'
    html_table_header += u'<th>日均发帖数（第三方链接贴）</th>'
    html_table_header += u'<th>日均评论数</th>'
    html_table_header += u'<th>日均点赞数</th>'
    html_table_header += u'<th>日均关注次数</th>'
    html_table_header += u'<th>日均关注用户数</th>'
    html_table_header += u'<th>周发帖用户数</th>'
    html_table_header += u'<th>周重复发帖用户数</th>'
    table_content.append(html_table_header)

    for row in data:
        html_row_str = u'<tr>'
        html_row_str += u'<td>%s</td>' %(row['week'])
        html_row_str += u'<td>%.0f</td>' %(row['view_pv'])
        html_row_str += u'<td>%.0f</td>' %(row['view_uv'])
        html_row_str += u'<td>%.0f</td>' %(row['click_pv'])
        html_row_str += u'<td>%.0f</td>' %(row['click_uv'])
        html_row_str += u'<td>%.2f</td>' %(row['view_pv_per'])
        html_row_str += u'<td>%.2f%%</td>' %(100.0*row['ctr'])
        html_row_str += u'<td>%.2f%%</td>' %(100.0*row['retain_rate'])
        html_row_str += u'<td>%.0f</td>' %(row['post_photo_count'])
        html_row_str += u'<td>%.0f</td>' %(row['post_link_count'])
        html_row_str += u'<td>%.0f</td>' %(row['comment_count'])
        html_row_str += u'<td>%.0f</td>' %(row['parise_count'])
        html_row_str += u'<td>%.0f</td>' %(row['attention_count'])
        html_row_str += u'<td>%.0f</td>' %(row['attention_people_count'])
        html_row_str += u'<td>%d</td>' %(row['post_user_count'])
        html_row_str += u'<td>%d</td>' %(row['repeat_post_user_count'])
        table_content.append(html_row_str)
    table_content.append(u'</table>')
    content = content + u'\n'.join(table_content)

add_table_community(weili_data_stats, u'微鲤头条社区相关数据统计')


# mailto_list = ["yinxiaofang@weli.cn","huangqian@weli.cn","lucong@weli.cn","guoyanlei@weli.cn","zhaomingyuan@weli.cn","fanrui@weli.cn"]
mailto_list = ["fanrui@weli.cn"]
mail_host = "smtp.zhwnl.cn"
mail_user = "non-reply@zhwnl.cn"
mail_pass = "!)($%^#!@asdfALKHDS9012"
def send_mail(to_list, sub, content):
    '''
    封装发送邮件服务,添加一个excel附件
    :param to_list: list,邮件联系人列表
    :param sub: str,邮件主题
    :param content: str,邮件内容
    :return: bool,发送是否成功
    '''
    me = "server" + "<" + mail_user + ">"
    # 构造MIMEMultipart对象做为根容器
    main_msg = email.MIMEMultipart.MIMEMultipart()
    # 设置根容器属性
    main_msg['From'] = me
    main_msg['To'] = ";".join(to_list)
    main_msg['Subject'] = sub
    # 构造MIMEText对象做为邮件显示内容并附加到根容器,正文部分
    text_msg = MIMEText(content, _subtype='html', _charset='utf-8')#must be html type
    main_msg.attach(text_msg)
    # 得到格式化后的完整文本
    msg = main_msg.as_string()
    server = smtplib.SMTP()
    try:
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(me, to_list, msg)
        server.close()
        return True
    except Exception, e:
        print str(e)
        return False

send_mail(mailto_list, u'微鲤头条周报数据统计邮件', content)
