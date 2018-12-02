# -*- encoding:utf-8 -*-
import logging

import sys
from optparse import OptionParser
import time
import datetime
import requests
import logging
import urllib
from requests.packages.urllib3.exceptions import InsecureRequestWarning


# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
azkabanIP = 'node1.azkaban.bigdata.dmp.com' # 'node1.azkaban.wnl.dmp.com'
azkabanPort = '8443'
username = 'fanrui'
password = '123123'
failureEmails = ''
session_id = ''
dependenceProjectsName = [u'sqoop_syc_stats',u'user_daily_new_active_stats',u'pv_event_stats']    # u'sqoop_syc_stats',u'user_daily_new_active_stats',u'custom_event_stats',u'pv_event_stats'
dependenceAzkabanProjects = []

class AzkabanProject:
    '所有 azkaban 项目的基类'

    def __init__(self, name, projectId, flows):
        self.name = name
        self.projectId = projectId
        self.flows = flows


def consoleOut():
    ''''' Output log to file and console '''
    # Define a Handler and set a format which output to file
    logging.basicConfig(
        level=logging.INFO,  # 定义输出到文件的log级别，大于此级别的都被输出
        format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',  # 定义输出log的格式
        datefmt='%Y-%m-%d %A %H:%M:%S')  # 时间


# 登录账号，获取 session_id
def login():
    global session_id
    param = {'action': 'login', 'username': username, 'password': password}
    r = requests.post(url="https://" + azkabanIP + ":" + azkabanPort, verify=False, data=param).json() #, headers= header
    if 'session.id' in r:
        session_id = r['session.id']
        logging.info(u'成功获取到session_id：' + session_id)
    else:
        logging.error(u'登录失败，获取session_id失败')
        if 'error' in r:
            logging.error(u'error信息：' + r['error'])
        else:
            logging.error(u'返回的信息：' + r['error'])
        sys.exit(1)


# 根据依赖项目名 获取依赖项目的所有 flow,将获取的对象 加到 dependenceAzkabanProjects 中
def fetchFlowsOfProject():
    global dependenceAzkabanProjects
    for projectName in dependenceProjectsName:
        param = {'ajax': 'fetchprojectflows', 'session.id': session_id, 'project': projectName}
        r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/manager", verify=False, params=param).json()
        flows = []
        if 'error' in r:
            logging.info(u'获取项目' + projectName + u'的Flows时失败，失败信息：' + r['error'])
        else:
            for flow in r['flows']:
                flows.append(flow['flowId'])
            dependenceAzkabanProjects.append(AzkabanProject(name=projectName, projectId=r['projectId'], flows=flows))
            logging.info(u'成功获取到' + projectName + u'项目的Flows：' + u','.join(flows))


# 根据依赖 项目名、flow名 获取所有依赖 flow的运行记录, 并获取flow 最终job的状态信息
# 所有依赖flow的最终job状态为 SUCCEEDED，才返回 True
def fetchFlowExecutions():
    for dependenceAzkabanProject in dependenceAzkabanProjects:
        for flow in dependenceAzkabanProject.flows:
            param = {'ajax': 'fetchFlowExecutions', 'session.id': session_id, 'project': dependenceAzkabanProject.name,
                     'flow': flow, 'start': 0, 'length': 1} # length = 1 表示获取依赖项目最新的1次执行记录
            r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/manager", verify=False, params=param).json()
            flowStatus = r['executions'][0]['status']

            todayTime = long(time.mktime(time.strptime(datetime.date.today().strftime('%Y%m%d'), '%Y%m%d'))) * 1000
            submitTime = long(r['executions'][0]['submitTime'])
            startTime = long(r['executions'][0]['startTime'])
            # 最近一次flow执行失败，且最近一次flow的执行时间是今天 ( 如果最近一次执行是今天之前，表示还未到达今天的调度时间 )
            # 则获取每个Job详细的失败信息，拿到那些执行成功的job
            if submitTime > todayTime and startTime > todayTime:
                if flowStatus == 'SUCCEEDED':   # flow运行成功，还要检测 该flow的最终job是否 成功
                    execid = r['executions'][0]['execId']
                    logging.info(u'依赖的' + dependenceAzkabanProject.name + u'项目的Flow：' + flow
                                 + u'执行id为' + str(execid) + u'，状态为SUCCEEDED，准备获取该flow最终job的状态')
                    param = {'ajax': 'fetchexecflow', 'session.id': session_id, 'execid': execid}
                    r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/executor", verify=False, params=param).json()
                    for job in r['nodes']:
                        if job['id'] == flow and job['status'] != 'SUCCEEDED':  # 依赖项目的最终job失败
                            logging.info(u'依赖的' + dependenceAzkabanProject.name + u'项目的Flow：' + flow + u'，最终job的状态为' + job['status'] + u'，所以该任务需要等待')
                            return False
                    logging.info(u'依赖的' + dependenceAzkabanProject.name + u'项目的Flow：' + flow
                                 + u'执行id为' + str(execid) + u'，该flow最后job成功，检查通过')
                else:
                    logging.info(u'依赖的' + dependenceAzkabanProject.name + u'项目的Flow：' + flow + u'，当前运行状态为：' + flowStatus + u'，所以该任务需要等待')
                    return False
            else:
                logging.info(u'依赖的' + dependenceAzkabanProject.name + u'项目的Flow：' + flow + u'今天还未进行调度，所以该任务需要等待')
                return False
    logging.info(u'所有依赖项目检查完毕，全部通过')
    return True


if __name__ == '__main__':
    consoleOut()
    flag = False
    count = 0
    while not flag:
        if count > 0:
            if count > 6:
                logging.info('检查' + str(count) + '次后，依赖仍然没有执行成功，不再等待，直接挂掉')
                sys.exit(1)
            logging.info('第' + str(count) + '次等待中。。。')
            time.sleep(600.0)
        count = count + 1

        dependenceAzkabanProjects = []
        login()                 # 登录账号,获取 session_id
        fetchFlowsOfProject()   # 根据项目名 获取项目对象
        flag = fetchFlowExecutions()   # 所有依赖都完成返回True，否则返回False

    logging.info('检查' + str(count) + '次后，所有依赖项目都执行成功，check通过')
    sys.exit(0)
