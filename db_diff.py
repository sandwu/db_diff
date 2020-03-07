import re

import pymysql


class EasyPyMySql:
    def __init__(self, config):
        self.conn = None
        self.cursor = None
        self.result = None
        self.errMsg = None #用于记录错误信息
        self.config = {
            "host": None,
            "port": None,
            "user": None,
            "password": None,
            "database": None,
            "charset": "utf8",
        }
        for k, v in config.items():
            if k in self.config:
                self.config[k] = v

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.py_close()

    def getConnection(self):
        config = self.config
        try:
            self.conn = pymysql.connect(**config)
            self.conn.autocommit = True
        except Exception as err:
            print(err)
            self.errMsg = "Connection error, please contact the Administrator !"
            raise
        return self.conn

    # 获取普通游标
    def getCursor(self):
        if self.conn is None:
            self.conn = self.getConnection()
        return self.conn.cursor(cursor=pymysql.cursors.DictCursor)

    # 执行sql语句
    def py_execute(self, sql, param):
        if self.cursor is None:
            self.cursor = self.getCursor()
        try:
            self.cursor.execute(sql, param)
            self.rows = self.cursor.fetchall()
        except Exception as err:
            self.errMsg = str(err)
            print(err)
            raise
        return self.rows

    # 关闭连接
    def py_close(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()


def get_localconf():
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "database": "test",
        "password": "123456"
    }

def get_remoteconf():
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "database": "test1",
        "password": "123456"
    }

def table_columns_to_dict(table_strucs):
    """
    将数据库取出的表结构转换为{table_name:{column_name:{}}}格式
    :param table_strucs:
    :return:
    """
    tmp = {}
    for table_struc in table_strucs:
        if table_struc["TABLE_NAME"] in tmp:
            tmp[table_struc["TABLE_NAME"]][table_struc["COLUMN_NAME"]] = table_struc
        else:
            tmp[table_struc["TABLE_NAME"]] = {}
            tmp[table_struc["TABLE_NAME"]][table_struc["COLUMN_NAME"]] = table_struc
    return tmp

def table_statistics_to_dict(table_strucs):
    """
    将数据库取出的表结构转换为{table_name:{index_name:{}}}格式
    :param table_strucs:
    :return:
    """
    tmp = {}
    for table_struc in table_strucs:
        if table_struc["TABLE_NAME"] in tmp:
            if table_struc["INDEX_NAME"] in tmp[table_struc["TABLE_NAME"]]:
                # 如果是联合索引，则将联合索引的字段合并在一起
                tmp[table_struc["TABLE_NAME"]][table_struc["INDEX_NAME"]]["COLUMN_NAME"] += ",%s" % table_struc["COLUMN_NAME"]
            else:
                tmp[table_struc["TABLE_NAME"]][table_struc["INDEX_NAME"]] = table_struc
        else:
            tmp[table_struc["TABLE_NAME"]] = {}
            tmp[table_struc["TABLE_NAME"]][table_struc["INDEX_NAME"]] = table_struc
    return tmp


def get_table_info(table_list, conf):
    """
    根据本地的table_list，依次找到远程数据库的表、字段、索引等信息，通过in table_list只查询存在的表
    :param table_list:
    :return:
    """
    db_name = conf["database"]

    if len(table_list) >1:
        sql_columns = "select TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE," \
                      "COLUMN_TYPE,EXTRA,COLUMN_COMMENT " \
                      "from information_schema.columns where table_schema=%s and table_name in %s"
        #生产环境 暂去除索引注释比较：index_comment
        sql_statistics = "select TABLE_NAME,NON_UNIQUE,INDEX_NAME,COLUMN_NAME,NULLABLE,INDEX_TYPE,SUB_PART " \
                     "from information_schema.statistics where table_schema=%s and table_name in %s"

        #单独取出参数，防sql注入
        sql_columns_params = (db_name, table_list)
        sql_statistics_params = (db_name, table_list)
    else:
        sql_columns = "select TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE," \
                      "COLUMN_TYPE,EXTRA,COLUMN_COMMENT " \
                      "from information_schema.columns where table_schema=%s and table_name=%s"
        # 生产环境 暂去除索引注释比较：index_comment
        sql_statistics = "select TABLE_NAME,NON_UNIQUE,INDEX_NAME,COLUMN_NAME,NULLABLE,INDEX_TYPE,SUB_PART " \
                         "from information_schema.statistics where table_schema=%s and table_name=%s"

        sql_columns_params = (db_name, table_list[0])
        sql_statistics_params = (db_name, table_list[0])

    try:
        db = EasyPyMySql(conf)
        res_columns = db.py_execute(sql_columns,sql_columns_params)  #防SQL注入
        res_statistics = db.py_execute(sql_statistics,sql_statistics_params)
        db.py_close()
        return res_columns, res_statistics
    except Exception as e:
        print(e)
    return None,None #错误返回

def multi_table_diff(select_table_list):
    #保存原始数据，进行比对
    local_conf = get_localconf()
    remote_conf = get_remoteconf()
    remote_res_columns, remote_res_statistics = get_table_info(select_table_list, remote_conf)
    select_res_columns, select_res_statistic = get_table_info(select_table_list, local_conf)

    #转换为{table:{}}的形式，根据dict.keys()确认table的是否存在以及逐一比对
    remote_column_dict = table_columns_to_dict(remote_res_columns)
    remote_statistics_dict = table_statistics_to_dict(remote_res_statistics)
    select_column_dict = table_columns_to_dict(select_res_columns)
    select_statistics_dict = table_statistics_to_dict(select_res_statistic)

    res_data = [] #定义返回结果
    if not remote_res_columns: #即远程无数据，说明所选的table都是需要创建
        try:
            db = EasyPyMySql(local_conf)
            for select_table in select_table_list:
                tmp = {"select_table": select_table, "remote_table": "", "diff_sql": "", "msg": ""} #自定义返回形式
                show_create_sql = 'show create table `%s`' % select_table
                diff_sql = db.py_execute(show_create_sql,None)[0]["Create Table"]
                tmp["diff_sql"] = re.sub("\n", "", diff_sql)  #将一些无用字段剔除，包括auto_increment这个用于定位当前表的开始点
                tmp["diff_sql"] = re.sub("AUTO_INCREMENT=[0-9]*", "", tmp["diff_sql"])
                tmp["diff_sql"] = re.sub("COLLATE\s[A-Za-z0-9_]*", "", tmp["diff_sql"])
                tmp["msg"] = "目标库无该表，生成表创建语句！"
                res_data.append(tmp)
            db.py_close()
        except Exception as e:
            print(e)
    else:
        for select_table in select_table_list:
            tmp = {"select_table": select_table, "remote_table": "", "diff_sql": "", "msg": ""}
            if select_table in remote_column_dict.keys():
                # 如果远程存在该表的话，执行对比，从表字段、表索引
                diff_sql = get_table_diff_result(select_table, remote_column_dict, remote_statistics_dict,
                                                 select_column_dict, select_statistics_dict)
                tmp["remote_table"] = select_table
                tmp["diff_sql"], alter_msg = diff_sql
                tmp["msg"] = "两张表的字段、索引一致！" if not tmp["diff_sql"] else alter_msg

            else:
                # 生成该表的创建语句，后续优化可以：添加到待执行的sql列表，统一执行
                try:
                    show_create_sql = "show create table `%s`" % select_table
                    db = EasyPyMySql(local_conf)
                    diff_sql = db.py_execute(show_create_sql, None)[0]["Create Table"]
                    db.py_close()
                    tmp["diff_sql"] = re.sub("\n", "", diff_sql)
                    tmp["diff_sql"] = re.sub("AUTO_INCREMENT=[0-9]*", "", tmp["diff_sql"])
                    tmp["diff_sql"] = re.sub("COLLATE\s[A-Za-z0-9_]*", "", tmp["diff_sql"])
                    tmp["msg"] = "目标库无该表，生成表创建语句！"
                except Exception as e:
                    print(e)
            res_data.append(tmp)
    data = {"res_data": res_data}
    return data


def table_diff_create_sql(row):
    """
    将表对比的结果生成对应的sql语句
    :param row:
    :return:
    """
    sql = ""
    if "unsigned" in row["COLUMN_TYPE"]:
        sql += " unsigned"
    if "zerofill" in row["COLUMN_TYPE"]:
        sql += " zerofill"
    if row["IS_NULLABLE"] == "NO":
        sql += " not null"
    if row["COLUMN_DEFAULT"]:
        if row.get("DATA_TYPE") in ["char", "varchar", "datetime", "date", "timestamp", "text", "longtext"]:
            if 'CURRENT_TIMESTAMP' in row["COLUMN_DEFAULT"] and row['EXTRA'] == 'on update CURRENT_TIMESTAMP':
                sql += " default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP "
            elif 'CURRENT_TIMESTAMP' in row["COLUMN_DEFAULT"]:
                sql += " default %(COLUMN_DEFAULT)s" % (row)
            else:
                sql += " default '%(COLUMN_DEFAULT)s'" % (row)
        else:

            sql += " default %(COLUMN_DEFAULT)s" % (row)
    else:
        if row.get("DATA_TYPE") in ["char", "varchar", "datetime", "date", "timestamp", "text", "longtext"]:
            if row.get("COLUMN_DEFAULT") == '""' or row.get("COLUMN_DEFAULT") == "''" or row.get(
                    "COLUMN_DEFAULT") == "" or row.get("COLUMN_DEFAULT") == '':
                sql += " default '' "
            # 注意这里无法区分是否有default值，因为设置default null和不设置default值读取COLUMN_DEFAULT都是None
            # 所以默认添加default Null
            else:
                # timestamp默认值不能是NULL，必须要加上NULL在前
                if row.get("DATA_TYPE") == "timestamp":
                    sql += " NULL default NULL "
                else:
                    sql += " default NULL "
        # 判断是主键的情况或者自增
        elif row.get("COLUMN_KEY") == "PRI" or "auto_increment" in row["EXTRA"]:
            pass
        else:
            sql += " default NULL "
    if "auto_increment" in row["EXTRA"]:
        sql += " auto_increment"
    if row["COLUMN_COMMENT"]:
        sql += " comment '%(COLUMN_COMMENT)s'" % (row)
    sql += ","
    return sql

def get_table_diff_result(select_table, remote_column_dict,remote_statistics_dict,select_column_dict,select_statistics_dict):
    """
    比对表差异，得到结果返回multi_table_diff
    :param remote_column_dict:
    :param remote_statistics_dict:
    :param select_column_dict:
    :param select_statistics_dict:
    :return:
    """
    #准确定义返回msg，帮助前端快速定位执行内容
    alter_msg = ""
    alter_modify_column_msg = ""
    alter_add_column_msg = ""
    alter_add_index_msg = ""
    alter_drop_index_msg = ""
    #判断是否有变更sql
    change_sql = ""
    #判断diff差异是否是该参数引起的
    ORDINAL_POSITION_flag = 0
    # 判断字段间区别
    for select_column_name in select_column_dict[select_table].keys():
        if select_column_name in remote_column_dict[select_table].keys():
            #执行对比表字段，获取对应table的字段数据和索引数据
            select_column_name_dict = select_column_dict[select_table][select_column_name]
            remote_column_name_dict = remote_column_dict[select_table][select_column_name]
            differ = set(select_column_name_dict.items()) ^ set(remote_column_name_dict.items())

            if not differ:
                continue
            else:
                # mac自带的mysql没有该字段，所以跳过，后续上线可以废除该判断！˚
                if differ == {('GENERATION_EXPRESSION', '')}:
                    continue
                #用于判断diff=ORDINAL_POSITION的情况，该参数服务于列所在的位置，暂不支持跳过
                for diff in differ:
                    if diff[0] =='ORDINAL_POSITION' :
                        ORDINAL_POSITION_flag = 1
                        break
                    else:
                        ORDINAL_POSITION_flag = 0
                if ORDINAL_POSITION_flag:
                    continue
                row = select_column_name_dict
                change_sql += " modify `%(COLUMN_NAME)s` %(COLUMN_TYPE)s" % row
                change_sql += table_diff_create_sql(row)
                alter_modify_column_msg += "`%(COLUMN_NAME)s`, " % row
        else:
            #没有该字段，生成该sql语句
            row = select_column_dict[select_table][select_column_name]
            change_sql += " add column `%(COLUMN_NAME)s` %(COLUMN_TYPE)s" % row
            change_sql += table_diff_create_sql(row)
            alter_add_column_msg += "`%(COLUMN_NAME)s`, " % row

    if alter_modify_column_msg:
        alter_modify_column_msg = alter_modify_column_msg.strip(", ")
        alter_msg += "修改字段：" + alter_modify_column_msg + "\n"

    if alter_add_column_msg:
        alter_add_column_msg = alter_add_column_msg.strip(", ")
        alter_msg += "增加字段：" + alter_add_column_msg + "\n"

    # 判断远程是否有多的column，有的话执行删除
    differ_columns = set(remote_column_dict[select_table].keys()) - set(select_column_dict[select_table].keys())
    if differ_columns:
        alter_drop_column_msg = ""
        for differ_column in differ_columns:
            change_sql += " drop column `%s`," % differ_column
            alter_drop_column_msg += " %s, " % differ_column
        alter_drop_column_msg = alter_drop_column_msg.strip(", ")
        alter_msg += "删除字段：" + alter_drop_column_msg + "\n"

    # 判断索引间区别
    for select_statistics_index_name in select_statistics_dict[select_table].keys():
        if select_statistics_index_name == "PRIMARY":
            if select_statistics_dict[select_table]["PRIMARY"]["COLUMN_NAME"] != remote_statistics_dict[select_table]["PRIMARY"]["COLUMN_NAME"]:
                change_sql += " drop primary key,add primary key(`%s`)," % select_statistics_dict[select_table]["PRIMARY"]["COLUMN_NAME"]
                alter_msg += "删除主键：" + select_statistics_dict[select_table]["PRIMARY"]["COLUMN_NAME"] + "\n"
        elif select_statistics_index_name in remote_statistics_dict[select_table].keys():
            #比对索引类型和索引字段是否一致
            select_index_name_dict = select_statistics_dict[select_table][select_statistics_index_name]
            remote_index_name_dict = remote_statistics_dict[select_table][select_statistics_index_name]
            differ = set(select_index_name_dict.items()) ^ set(remote_index_name_dict.items())
            if not differ or differ == {('NULLABLE', ''), ('NULLABLE', 'YES')}:
                continue
            else:
                change_sql += " drop index `%s`," % select_statistics_index_name
                alter_drop_index_msg += " %s, " % select_statistics_index_name
                if select_statistics_dict[select_table][select_statistics_index_name]["NON_UNIQUE"] == 0:
                    if select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"] and isinstance(select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"],int):
                        change_sql += "add unique `%s` (%s(%d))," % (
                            select_statistics_index_name,
                            select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"],
                            select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"]
                        )
                    else:
                        change_sql += "add unique `%s` (%s)," % (
                            select_statistics_index_name,
                            select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"]
                        )
                else:
                    if select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"] and isinstance(select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"],int):
                        change_sql += "add index `%s` (%s(%d))," % (
                            select_statistics_index_name,
                            select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"],
                            select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"]
                        )
                    else:
                        change_sql += "add index `%s` (%s)," % (
                            select_statistics_index_name,
                            select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"]
                        )

        else:
            alter_add_index_msg += " %s, " % select_statistics_index_name
            if select_statistics_dict[select_table][select_statistics_index_name]["NON_UNIQUE"] == 0:
                # 增加前缀索引判断
                if select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"] and isinstance(
                        select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"], int):
                    change_sql += "add unique `%s` (%s(%d))," % (
                        select_statistics_index_name,
                        select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"],
                        select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"]
                    )
                else:
                    change_sql += "add unique `%s` (%s)," % (
                        select_statistics_index_name,
                        select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"]
                    )
            else:
                #增加前缀索引判断
                if select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"] and isinstance(
                        select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"], int):
                    change_sql += "add index `%s` (%s(%d))," % (
                        select_statistics_index_name,
                        select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"],
                        select_statistics_dict[select_table][select_statistics_index_name]["SUB_PART"]
                    )
                else:
                    change_sql += "add index `%s` (%s)," % (
                        select_statistics_index_name,
                        select_statistics_dict[select_table][select_statistics_index_name]["COLUMN_NAME"]
                    )

    # 判断远程是否有多的index，有的话执行删除
    differ_statistics = set(remote_statistics_dict[select_table].keys()) - set(select_statistics_dict[select_table].keys())
    if differ_statistics:
        for differ_statistic in differ_statistics:
            alter_drop_index_msg += " %s, " % differ_statistic
            change_sql += " drop index `%s`," % differ_statistic

    if alter_drop_index_msg:
        alter_drop_index_msg = alter_drop_index_msg.strip(", ")
        alter_msg += "删除索引：" + alter_drop_index_msg + "\n"

    if alter_add_index_msg:
        alter_add_index_msg = alter_add_index_msg.strip(", ")
        alter_msg += "增加索引：" + alter_add_index_msg + "\n"

    if change_sql:
        table_diff_sql = "alter table `%s`" % select_table
        table_diff_sql += change_sql.rstrip(",")
    else:
        table_diff_sql = ""
    return (table_diff_sql,alter_msg)


if __name__ == "__main__":
    select_table_list = ["t","t4","t2","t3"]
    diff_sql = multi_table_diff(select_table_list)
    print(diff_sql)



















