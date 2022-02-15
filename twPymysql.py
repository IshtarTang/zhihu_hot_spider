# coding=utf-8
import pymysql
import time
import traceback
import logging

logging.basicConfig(level="DEBUG", datefmt='%y-%m-%d %H:%M:%S', format="[%(levelname)s] %(asctime)s  %(message)s")


class Connection(object):

    def __init__(self, host, user, password, database, port=3306,
                 max_idle_time=7 * 3600, connect_timeout=10, charset="utf8mb4"):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)
        args = dict(host=host,
                    user=user,
                    password=password,
                    port=port,
                    database=database,
                    charset=charset,
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=connect_timeout,
                    autocommit=True
                    )
        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        self.reconnect()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _ensure_connected(self):
        # 默认情况下，客户端空闲8小时mysql会关闭连接，而客户端并不会报告，会直接在操作时报错，所以每隔7小时重连一次
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def reconnect(self):
        self.close()
        self._db = pymysql.connect(**self._db_args)

    def close(self):
        # 用getattr避免init没跑完就del导致暴毙
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def __del__(self):
        self.close()

    # ============查询方法===============
    def getOne(self, query, *parameters, **kwparameters):
        # select 获取一行
        cursor = self._cursor()
        try:
            cursor.execute(query, parameters or kwparameters)
            result = cursor.fetchone()
            return result
        finally:
            cursor.close()

    def getMany(self, sql, size=0, *parameters, **kwparameters):
        # 获取一堆
        curosr = self._cursor()
        try:
            curosr.execute(sql, kwparameters or parameters)
            if size:
                result = curosr.fetchmany(size)
            else:
                result = curosr.fetchall()
            return result
        finally:
            curosr.close()

    def execute(self, sql, *parameters, **kwparameters):
        cursor = self._cursor()
        changed_num = 0
        try:
            changed_num = cursor.execute(sql, kwparameters or parameters)
        except Exception as e:
            if e.args[0] == 1062:
                # 主键重复，继续运行
                pass
            else:
                traceback.print_exc()
                raise e
        finally:
            cursor.close()

        return changed_num

    def table_has(self, table_name, field, value):
        sql = "select * from {} where {} = {}".format(table_name, field, value)
        result = self.getOne(sql)
        return result

    def table_insert(self, table_name, fild_item):
        # item is a dict : key is mysql table field
        fields = list(fild_item.keys())
        values = list(fild_item.values())
        fieldsstr = ",".join(fields)
        valstr_placeholder = ",".join(["%s"] * len(fild_item))
        sql = "insert into %s(%s) values(%s)" % (table_name, fieldsstr, valstr_placeholder)
        try:
            self.execute(sql, *values)
        except Exception as e:
            print("read to raise")
            if e.args[0] == 1062:
                raise Exception("The primary key repeat")
                # 主键重复
                pass
            else:
                traceback.print_exc()
                raise

