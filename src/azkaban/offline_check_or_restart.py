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

# 禁用策略选择(此api进行的调度会禁用一些之前已经执行成功的job)，可选择的策略有：depend, direct 。
# direct 表示将状态为 SUCCEEDED 或者 SKIPPED 的job做disabled操作
# depend 表示 不仅将状态为 SUCCEEDED 或者 SKIPPED 的job做disabled操作，而且将这些job依赖的job也做 disabled操作
disabledStrategy = 'depend' # 可选择的策略有：depend, direct

# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
azkabanIP = 'node1.azkaban.bigdata.dmp.com'
azkabanPort = '8443'
username = 'fanrui'
password = '123123'
failureEmails = ''
session_id = ''
projectsName = [u'sqoop_syc_stats', u'custom_event_stats', u'pv_event_stats', u'web_event_stats',
                u'user_daily_new_active_stats', u'swap_cleanup', u'weili_lizhi_stats',u'weili_lizhi_stats_1w',
                u'weili_tmp_stats', u'marketing_channel_daily', u'weili_risk_user_stats',
                u'adx_ad_stats',u'data_api',u'leopard_user_stats_1d',u'monitor']    # u'sqoop_syc_stats',u'user_daily_new_active_stats',u'custom_event_stats',u'pv_event_stats'
projectsMaxTimeMap = {u'sqoop_syc_stats':80, u'custom_event_stats':60, u'pv_event_stats':150, u'web_event_stats':20,
                u'user_daily_new_active_stats':50,u'swap_cleanup':100,  u'weili_lizhi_stats':50,u'weili_lizhi_stats_1w':30,
                u'weili_tmp_stats':20, u'marketing_channel_daily':20, u'weili_risk_user_stats':30,
                u'adx_ad_stats':90,u'data_api':30,u'leopard_user_stats_1d':30,u'monitor':10}
azkabanProjects = []
retryFlows = []         # 检测到运行失败需要重试的 execid 和 对应的执行成功的job
restart_flag = False    # 启动任务是否存在失败的情况
kill_flag = False       # 是否存在kill的任务

class AzkabanProject:
    '所有 azkaban 项目的基类'

    def __init__(self, name, projectId, flows):
        self.name = name
        self.projectId = projectId
        self.flows = flows


class ExecuteFlow:
    '要执行的flow的信息'

    def __init__(self, project, flow, disabled):
        self.project = project
        self.flow = flow
        self.disabled = '[\"' + '\",\"'.join(disabled) + '\"]'
        # self.disabled = disabled

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


# 根据项目名 获取项目的所有 flow,将获取的对象 加到 azkabanProjects 中
def fetchFlowsOfProject():
    for projectName in projectsName:
        param = {'ajax': 'fetchprojectflows', 'session.id': session_id, 'project': projectName}
        r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/manager", verify=False, params=param).json()
        flows = []
        if 'error' in r:
            logging.info(u'获取项目' + projectName + u'的Flows时失败，失败信息：' + r['error'])
        else:
            for flow in r['flows']:
                flows.append(flow['flowId'])
            azkabanProjects.append(AzkabanProject(name=projectName, projectId=r['projectId'], flows=flows))
            logging.info(u'成功获取到' + projectName + u'项目的Flows：' + u','.join(flows))


# 根据依赖关系，通过递归，把jobName及其依赖的job添加到成功的 succeededJobs 中
def recursionAddDependence(succeededJobs, dependenceJobs, jobName):
    if jobName not in succeededJobs:    # 当前job不在成功的job列表中
        succeededJobs.append(jobName)
        if jobName in dependenceJobs:   # 当前job有依赖的其他job
            for dependenceJob in dependenceJobs[jobName]:   # 遍历，将其依赖的job添加到succeededJobs中
                recursionAddDependence(succeededJobs, dependenceJobs, dependenceJob)


# 构建Job依赖关系，将需要执行的 ExecuteFlow 对象添加到 retryFlows 中
def addJob(r):
    global retryFlows
    succeededJobs = []
    if disabledStrategy == 'depend':
        dependenceJobs = {}
        for job in r['nodes']:  # 遍历所有job，构建所有job之间的依赖关系
            if 'in' in job:   # 这个job有依赖的job
                dependenceJobs[job['id']] = job['in']

    for job in r['nodes']:  # 遍历所有job，将成功的job添加到succeededJobs中
        if job['status'] == 'SUCCEEDED' or job['status'] == 'SKIPPED':  # 此job已经执行成功了，把该job和该job的所有依赖 加入到 succeededJobs
            if disabledStrategy == 'depend':
                recursionAddDependence(succeededJobs, dependenceJobs, job['id'])
            elif disabledStrategy == 'direct':
                succeededJobs.append(job['id'])
    retryFlows.append(ExecuteFlow(project=r['project'], flow=r['flow'], disabled=succeededJobs))
    logging.info(r['project'] + u'项目的Flow：' + r['flow']
                 + u'，之前已经执行完成Job为：' + ','.join(succeededJobs) )


# kill掉正在运行的flow
def killRuningFlow(execid):
    param = {'ajax': 'cancelFlow', 'session.id': session_id, 'execid': execid}
    r = requests.post(url="https://" + azkabanIP + ":" + azkabanPort + "/executor", verify=False, data=param).json()
    if 'error' in r:
        logging.info(u'kill [' + str(execid) + u']execid时失败,reason:' + r['error'])
    else:
        logging.info(u'成功kill execid:' + str(execid) )


# 根据 项目名、flow名 获取该flow的运行记录, 并获取flow 每个job的详细失败信息，构建 retryFlows 列表
def fetchFlowExecutions():
    global kill_flag
    todayTime = long(time.mktime(time.strptime(datetime.date.today().strftime('%Y%m%d'), '%Y%m%d'))) * 1000
    currentTime = long(round(time.time()* 1000))
    for azkabanProject in azkabanProjects:
        for flow in azkabanProject.flows:
            param = {'ajax': 'fetchFlowExecutions', 'session.id': session_id, 'project': azkabanProject.name,
                     'flow': flow, 'start': 0, 'length': 1} # length = 1 表示获取最新的1次执行记录
            r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/manager", verify=False, params=param).json()
            flowStatus = r['executions'][0]['status']

            submitTime = long(r['executions'][0]['submitTime'])
            startTime = long(r['executions'][0]['startTime'])
            # 最近一次flow执行失败，且最近一次flow的执行时间是今天 ( 如果最近一次执行是今天之前，表示还未到达今天的调度时间 )
            # 则获取每个Job详细的失败信息，拿到那些执行成功的job
            if submitTime > todayTime and startTime > todayTime:
                execid = r['executions'][0]['execId']
                if flowStatus != 'SUCCEEDED' and flowStatus != 'RUNNING':
                    logging.info(azkabanProject.name + u'项目的Flow：' + flow
                                 + u'执行id为' + str(execid)
                                 + u'，状态为' + flowStatus
                                 + u'，准备获取该flow执行成功的job')
                    param = {'ajax': 'fetchexecflow', 'session.id': session_id, 'execid': execid}
                    r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/executor", verify=False, params=param).json()
                    addJob(r)
                else:
                    if flowStatus == 'SUCCEEDED' :
                        logging.info(azkabanProject.name + u'项目的Flow：' + flow + u'，当前运行状态为：' + flowStatus + u'，不需要再进行调度')
                    else :
                        duration = (currentTime - startTime)/1000/60
                        if duration < projectsMaxTimeMap[azkabanProject.name]:
                            logging.info(r['executions'][0])
                            logging.info(azkabanProject.name + u'项目的Flow：' + flow + u'，当前运行状态为：RUNNING，且运行时间较短，不需要再进行调度')
                        else:
                            logging.error(azkabanProject.name + u'项目的Flow：' + flow + u'，执行id为：'+ str(execid) +u'当前运行状态为：RUNNING，且运行时间较长（' + str(duration) + u'min），准备将其kill')
                            killRuningFlow(execid)
                            kill_flag = True
                            param = {'ajax': 'fetchexecflow', 'session.id': session_id, 'execid': execid}
                            r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/executor", verify=False, params=param).json()
                            addJob(r)
            else:
                logging.info(azkabanProject.name + u'项目的Flow：' + flow + u'，还未到达今天的调度时间')


# 重试 retryFlows 列表中所有的 Flow
def executeFlows():
    global restart_flag
    for retryFlow in retryFlows:
        param = {'ajax': 'executeFlow', 'session.id': session_id, 'project': retryFlow.project,
                 'flow': retryFlow.flow, 'failureAction': 'finishPossible', 'disabled': retryFlow.disabled
                 }
        r = requests.get(url="https://" + azkabanIP + ":" + azkabanPort + "/executor", verify=False, params=param ).json()
        if 'error' in r:
            logging.error(retryFlow.project + u'项目的Flow：' + retryFlow.flow
                         + u'执行失败，错误提示：' + r['error'])
            restart_flag = True
        else:
            logging.info(retryFlow.project + u'项目的Flow：' + retryFlow.flow
                         + u'执行提交成功，执行id：' + str(r['execid']))

# 使用 crontab 进行定时调度
if __name__ == '__main__':
    consoleOut()
    login()                 # 登录账号,获取 session_id
    fetchFlowsOfProject()   # 根据项目名 获取项目对象
    fetchFlowExecutions()   # 根据项目对象 构建需要重试的 retryFlows 列表
    if kill_flag:
        time.sleep(20.0)
    executeFlows()          # 执行 retryFlows 中所有需要重试的flow
    if restart_flag:        # 有重启失败的任务
        sys.exit(1)
    else :
        sys.exit(0)
