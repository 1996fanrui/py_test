# -*- encoding:utf-8 -*-
import logging

import sys
from optparse import OptionParser
import datetime
import requests
import logging

username = 'fanrui'
password = '123123'
session_id = ''
projectsName = ['wnl_user_daily_new_active_stats', 'wnl_custom_event_stats']
azkabanProjects = []
retryExecid = {}    # 检测到运行失败需要重试的 execid 和 对应的执行成功的job

class AzkabanProject:
    '所有azkaban项目的基类'

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
    # header = {"Content-Type": "application/x-www-form-urlencoded; charset=utf-8", "X-Requested-With": "XMLHttpRequest"}
    r = requests.post(url="https://10.19.74.215:8443", verify=False, data=param).json() #, headers= header
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


# 根据项目名 获取项目的所有 flow
def fetchFlowsOfProject():
    for projectName in projectsName:
        param = {'ajax': 'fetchprojectflows', 'session.id': session_id, 'project': projectName}
        r = requests.get(url="https://10.19.74.215:8443/manager", verify=False, params=param).json()
        flows = []
        for flow in r['flows']:
            flows.append(flow['flowId'])
        azkabanProjects.append(AzkabanProject(name=projectName, projectId=r['projectId'], flows=flows))


# 通过递归，把jobName及其依赖的job添加到成功的 succeededJobs 中
def recursionAddDependence(succeededJobs, dependenceJobs, jobName):
    if jobName not in succeededJobs:    # 当前job不在成功的job列表中
        succeededJobs.append(jobName)
        if jobName in dependenceJobs:   # 当前job有依赖的其他job
            for dependenceJob in dependenceJobs[jobName]:   # 遍历，将其依赖的job添加到succeededJobs中
                recursionAddDependence(succeededJobs, dependenceJobs, dependenceJob)


# 根据依赖关系，将成功的job和成功job依赖的job添加到 retryExecid 中
def addJob(execid, r):
    global retryExecid
    succeededJobs = []
    dependenceJobs = {}
    for job in r['nodes']:  # 遍历所有job，构建所有job之间的依赖关系
        if 'in' in job:   # 这个job有依赖的job
            dependenceJobs[job['id']] = job['in']

    for job in r['nodes']:  # 遍历所有job，将成功的job添加到succeededJobs中
        if job['status'] == 'SUCCEEDED':  # 此job已经执行成功了，把该job和该job的所有依赖 加入到 succeededJobs
            recursionAddDependence(succeededJobs, dependenceJobs, job['id'])
    retryExecid[execid] = succeededJobs


# 根据 项目名、flow名 获取该flow的运行记录
def fetchFlowExecutions():
    for azkabanProject in azkabanProjects:
        for flow in azkabanProject.flows:
            param = {'ajax': 'fetchFlowExecutions', 'session.id': session_id, 'project': azkabanProject.name,
                     'flow': flow, 'start': 0, 'length': 1} # 1表示获取最新的1次执行记录
            r = requests.get(url="https://10.19.74.215:8443/manager", verify=False, params=param).json()
            print r
            print r['executions'][0]['status']
            if r['executions'][0]['status'] == 'SUCCEEDED':     # 该flow执行失败，获取每个Job详细的失败信息
                execid = r['executions'][0]['execId']
                param = {'ajax': 'fetchexecflow', 'session.id': session_id, 'execid': execid}
                r = requests.get(url="https://10.19.74.215:8443/executor", verify=False, params=param).json()
                addJob(execid, r)


# 使用 crontab 进行定时调度
if __name__ == '__main__':
    consoleOut()
    login()
    fetchFlowsOfProject()
    fetchFlowExecutions()

    print retryExecid
