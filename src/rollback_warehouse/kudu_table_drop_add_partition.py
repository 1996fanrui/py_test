#!/usr/bin/python
# coding=utf-8

from optparse import OptionParser

# 该脚本用来快速生成 1、kudu表删除、增加分区 2、删除增加分区的azkaban调度flow 3、 从kudu导数据到hive的调度flow
# python util/kudu_table_drop_add_partition.py -d prod/projects/adx_ad_stats -t prod.ods_kudu_adx_v3_log_track_data_1d,prod.ods_kudu_adx_v3_billing_log_1d,prod.ods_kudu_adx_v3_client_request_1d,prod.ods_kudu_adx_v3_client_request_channel_1d,prod.ods_kudu_adx_v3_demand_request_1d,prod.ods_kudu_adx_v3_demand_request_ad_info_1d

class Table:
    'Kudu Table的基类'
    def __init__(self, table):
        self.tableAll = table
        if '.' in table:
            tt = table.split('.')
            self.database = tt[0]
            self.tableName = tt[1]
        else:
            self.database = ''
            self.tableName = table


def editFile(fileName,fileContent):
    # 打开一个文件
    fo = open(fileName, "w")
    fo.write( fileContent)
    # 关闭打开的文件
    fo.close()


def getAddPartitionJobName(tableName):
    return 'alter_' + tableName + '_add_partition'


def getDropPartitionJobName(tableName):
    return 'alter_' + tableName + '_drop_partition'


def kuduTableNameToHiveTable(kuduTableName):
    return kuduTableName.replace('kudu','hive')


def addParition(dir,tableNow,tableLast):
    jobName = getAddPartitionJobName(tableNow.tableName)
    sqlFileName = dir + '/sql/' + jobName + '.sql'
    flowFileName = dir + '/flow/' + jobName + '.job'
    addSql = "alter table "+ tableNow.tableAll + " add range partition value = '${5_yyyymmdd}';"
    editFile(sqlFileName,addSql)

    flowContent = "#" + jobName + "\ntype=command\n"

    if tableLast is not None:
        flowContent += "dependencies="
        flowContent += getDropPartitionJobName(tableLast.tableName) + "\n"

    flowContent += "command=python /data/dmp/warehouse-plus/prod/projects/impala_run_job.py -f /data/dmp/warehouse-plus/" + sqlFileName

    editFile(flowFileName,flowContent)


def dropParition(dir,tableNow):
    jobName = getDropPartitionJobName(tableNow.tableName)
    sqlFileName = dir + '/sql/' + jobName + '.sql'
    flowFileName = dir + '/flow/' + jobName + '.job'
    dropSql = "alter table "+ tableNow.tableAll + " drop range partition value = '${yyyymmdd_2}';"
    editFile(sqlFileName,dropSql)

    flowContent = "#" + jobName + "\ntype=command\ndependencies=" + getAddPartitionJobName(tableNow.tableName) + "\n" \
                  + "command=python /data/dmp/warehouse-plus/prod/projects/impala_run_job.py -f /data/dmp/warehouse-plus/" + sqlFileName

    editFile(flowFileName, flowContent)


def kuduToHive(dir,tableNow,tableLast):
    jobName = kuduTableNameToHiveTable(tableNow.tableName)
    sqlFileName = dir + '/sql/' + jobName + '.sql'
    flowFileName = dir + '/flow/' + jobName + '.job'
    flowContent = "#" + jobName + "\ntype=command\n"
    if tableLast is not None:
        flowContent += "dependencies="
        flowContent += kuduTableNameToHiveTable(tableLast.tableName) + "\n"
    flowContent += "command=python /data/dmp/warehouse-plus/prod/projects/impala_run_job.py -f /data/dmp/warehouse-plus/" + sqlFileName
    editFile(flowFileName,flowContent)

def generateTableFile(dir,tableNow,tableLast):
    addParition(dir,tableNow,tableLast)
    dropParition(dir,tableNow)
    kuduToHive(dir,tableNow,tableLast)



# @author:fanrui
if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("-t", help="kudu tables name, ',' delimiter")
    parser.add_option("-d", help="generate file dir")
    (options, args) = parser.parse_args()

    if options.t is None:
        print '必须指定kudu表名，且使用逗号分隔'
        sys.exit(1)

    if options.d is None:
        print '必须指定输出目录'
        sys.exit(1)

    dir = options.d
    tablesParam = options.t

    # dir = 'prod/projects/adx_ad_stats'
    # tablesParam = 'prod.ods_kudu_adx_v3_billing_log_1d,prod.ods_kudu_adx_v3_client_request_1d,prod.ods_kudu_adx_v3_client_request_channel_1d,prod.ods_kudu_adx_v3_demand_request_1d,prod.ods_kudu_adx_v3_demand_request_ad_info_1d'

    # tablesParam 解析为 Table 对象
    tables = []
    for table in tablesParam.split(','):
        tables.append(Table(table))

    # 生成 增加、删除分区的sql 和 flow 文件
    generateTableFile(dir, tables[0], None)
    for i in range(1,len(tables)):
        generateTableFile(dir,tables[i],tables[i-1])


