# coding=utf-8

from __future__ import print_function
import sys
import re

import MySQLdb
import MySQLdb.cursors


class DBUtil(object):
    """docstring for Database Util... And now it depends on mysqldb..."""

    def __init__(self, db_config=None, sp_config=None):
        """
            初始化工具类
        :param dict db_config:  一些数据库连接的信息,最好是写到配置文件里
        :param dict sp_config: 一些特殊的设置, level: 运行等级(run/debug), autocommit: 是否自动提交
        """
        assert isinstance(sp_config, dict)
        assert isinstance(db_config, dict)
        self._show_sql = DBUtil._level[sp_config.get('level', 'run')]
        self.host = db_config['HOST']
        self.port = db_config['PORT']
        self.user = db_config['USER']
        self.passwd = db_config['PASSWORD']
        self.dbname = db_config['NAME']
        self.charset = 'utf8'
        # 我感觉DictCursor蛮好用的,如果不喜欢,可以更换....
        self.cursorclass = MySQLdb.cursors.DictCursor
        self._auto_commit = sp_config.get('autocommit', True)
        self._conn = None
        self._connect()
        self._cursor = self._conn.cursor()

    _joiner_dict = {
        'lte': '<=',
        'gte': '>=',
        'neq': '!=',
        'eq': '=',
        'lt': '<',
        'gt': '>',
        'in': 'IN',
        'no_in': 'NOT IN',
    }

    def query(self, sql):
        """
            直接执行一条SQL
        :param str|unicode sql: SQL
        """
        self._cursor.execute(sql)

    def _connect(self):
        try:
            self._conn = MySQLdb.connect(host=self.host,
                                         port=self.port,
                                         user=self.user,
                                         passwd=self.passwd,
                                         db=self.dbname,
                                         charset=self.charset,
                                         cursorclass=self.cursorclass)
        except MySQLdb.Error as e:
            print(e, file=sys.stderr)
            print('Connect to database error!', file=sys.stderr)

    def get_cursor(self):
        """
            获得cursor
        :return:
        """
        return self._cursor

    def get_connection(self):
        """
            获得一个connection
        :return:
        """
        return self._conn

    def _execute(self, sql, value_list, change_db=False):
        if self._show_sql:
            print('SQL:{0}'.format(sql), file=sys.stdout)
            print('Params: {0}'.format(value_list), file=sys.stdout)
        try:
            self._cursor.execute(sql, value_list)
            if change_db and self._auto_commit:
                self.commit()
        except Exception as e:
            if change_db and self._auto_commit:
                self.rollback()
            print(e, file=sys.stderr)
            print('SQL:{0}'.format(sql), file=sys.stderr)
            print('Params: {0}'.format(value_list), file=sys.stderr)

    def _executemany(self, sql, value_lists, change_db=False):
        if self._show_sql:
            print('SQL:{0}'.format(sql), file=sys.stdout)
            print('Params: {0}'.format(value_lists), file=sys.stdout)
        try:
            self._cursor.executemany(sql, value_lists)
            if change_db and self._auto_commit:
                self.commit()
        except Exception as e:
            if change_db and self._auto_commit:
                self.rollback()
            print(e, file=sys.stderr)
            print('SQL:{0}'.format(sql), file=sys.stderr)
            print('Params: {0}'.format(value_lists), file=sys.stderr)

    def _select(self, table, field_str='COUNT(*)', cond_dict=None, extends='', limit_=None, offset_=None):
        key_str, value_list = self._process_cond_dict(cond_dict)
        sql = "SELECT {0:s} FROM {1:s} WHERE {2:s} {3:s}".format(field_str, table, key_str, extends)
        if limit_:
            sql = '{} LIMIT {}'.format(sql, limit_)
        if offset_:
            sql = '{} OFFSET {}'.format(sql, offset_)
        self._execute(sql, value_list, change_db=False)

    def _parse_key(self, key):
        if key is None or key == 'None' or key == '':
            raise DBUtilException('The key of condition dict should not be null.')
        result = re.search(r'(.+)__(.+)', key)
        if result:
            key = result.group(1)
            joiner = self._joiner_dict[result.group(2)]
        else:
            joiner = '='
        return key, joiner

    def _process_cond_dict(self, cond_dict):
        assert isinstance(cond_dict, dict), 'The condition_dict parameter should be a dict.'
        key_list = []
        value_list = []
        for key, value in [(str(tmp_key), tmp_value) for tmp_key, tmp_value in cond_dict.items()]:
            key, joiner = self._parse_key(key)
            if value is None or value == 'NULL':
                key_list.append('{0:s} IS NULL'.format(key))
            elif value == 'NOT NULL':
                key_list.append('{0:s} IS NOT NULL'.format(key))
            elif isinstance(value, (int, long, float,)):
                key_list.append('{0:s} {1:s} %s'.format(key, joiner))
                value_list.append(value * 1)
            elif joiner in ('IN', 'NOT IN'):
                key_list.append('{0:s} {1:s} ({2:s})'.format(key, joiner, ','.join(['%s'] * len(value))))
                value_list.extend(value)
            else:
                key_list.append('{0:s} {1:s} %s'.format(key, joiner))
                value_list.append(value)
        return ' AND '.join(key_list), value_list

    def select_one(self, table, fields=('*',), cond_dict=None, extends=''):
        """
            从数据库中查询一行
        :param str|unicode table:    表名
        :param list|tuple fields:    查询字段名
        :param dict cond_dict:       条件字典
        :param str|unicode extends:  扩展参数,如 GROUP BY
        :rtype: dict
        :return: 查询出的结果
        """
        extends += ' LIMIT 1'
        self._base_select(table, fields, cond_dict=cond_dict, extends=extends, )
        return self._cursor.fetchone()

    def select_many(self, table, fields=('*',), cond_dict=None, extends='', offset_=None, limit_=None):
        """
            从数据库中查询多行, 一次取完
        :param str|unicode table:     表名
        :param list|tuple fields:     查询字段名
        :param dict  cond_dict:       条件字典
        :param str|unicode extends:   扩展参数,如 GROUP BY
        :param int offset_:           从哪一个的下一个开始取
        :param int limit_:            取多少个
        :rtype: list
        :return: 查询出的数据
        """
        self._base_select(table, fields, cond_dict=cond_dict, extends=extends, offset_=offset_, limit_=limit_)
        return self._cursor.fetchall()

    def _base_select(self, table, fields=('*',), cond_dict=None, extends='', offset_=None, limit_=None):
        """
            从数据库中查询多行,
        :param str|unicode table:     表名
        :param list|tuple fields:     查询字段名
        :param dict  cond_dict:       条件字典
        :param str|unicode extends:   扩展参数,如 GROUP BY
        :param int offset_:           从哪一个的下一个开始取
        :param int limit_:            取多少个
        :rtype: None
        :return: None
        """
        fields_str = ','.join(fields)
        self._select(table, field_str=fields_str, cond_dict=cond_dict, extends=extends, limit_=limit_, offset_=offset_)

    def select_yield(self, table, fields=('*',), cond_dict=None, extends='', offset_=None, limit_=None, row_count=100):
        """
            从数据库中查询多行, 分批取出
        :param str|unicode table:     表名
        :param list|tuple fields:     查询字段名
        :param dict  cond_dict:       条件字典
        :param str|unicode extends:   扩展参数,如 GROUP BY
        :param int offset_:           从哪一个的下一个开始取
        :param int limit_:            取多少个
        :param int row_count:         每次取多少个
        """
        assert isinstance(row_count, int), 'param row_count type is wrong'
        assert row_count > 0, 'param row_count value is wrong'
        self._base_select(table, fields, cond_dict=cond_dict, extends=extends, offset_=offset_, limit_=limit_)
        while True:
            results = self._cursor.fetchmany(size=row_count)
            if not results:
                break
            yield results

    def insert_one(self, table, kv_dict):
        """
            插入一条数据
        :param str|unicode table:   表名
        :param dict kv_dict:        数据字典  {'column_1': d1, 'column2': d2}
        :return: 受影响的行数
        :rtype: int
        """
        assert isinstance(kv_dict, dict), 'The kv_dict parameter should be a dict.'
        placeholder_list = []
        values_list = []
        for value in kv_dict.values():
            placeholder_list.append('%s')
            if value is None or value == 'NULL':
                values_list.append(None)
            elif isinstance(value, (int, long, float,)):
                values_list.append(str(value * 1))
            else:
                values_list.append(u'{0}'.format(value))
        sql = "INSERT INTO {table} ( {columns} ) VALUES ( {placeholders} )".format(table=table,
                                                                                   columns=','.join(kv_dict.keys()),
                                                                                   placeholders=','.join(
                                                                                           placeholder_list))
        self._execute(sql, values_list, change_db=True)
        return self.affected_num()

    def insert_many(self, table, kv_dict):
        """
            一次插入多条数据
        :param str|unicode table:   表名
        :param dict kv_dict:        数据字典  {'column_name1': [d1, d2, d3...], 'column_name2': [x1, x2, x3...]}
        :return: 受影响的行数
        :rtype: int
        """
        assert isinstance(kv_dict, dict), 'The kv_dict parameter should be a dict.'
        keys = kv_dict.keys()
        values = kv_dict.values()
        len_first = len(values[0]) if values else 0
        if len(values) < 1 or len_first < 1:
            print('The insert value_list is empty.', file=sys.stdout)
            print(table, kv_dict)
            return
        same_len = all(len(i) == len_first for i in values)
        if not same_len:
            print('The lengths of all insert sequence are not same. Please check it.', file=sys.stdout)
            return
        value_lists = []
        placeholder_list = ['%s', ] * len(keys)
        for i in range(len_first):
            tmp_list = list()
            for value_list in values:
                value = value_list[i]
                if value is None or value == 'NULL':
                    tmp_list.append(None)
                elif isinstance(value, (int, long, float,)):
                    tmp_list.append(value * 1)
                else:
                    tmp_list.append(value)
            value_lists.append(tmp_list)

        sql = "INSERT INTO {table} ( {columns} ) VALUES ( {placeholders} ) ".format(table=table,
                                                                                    columns=' , '.join(keys),
                                                                                    placeholders=','.join(
                                                                                            placeholder_list))
        self._executemany(sql, value_lists, change_db=True)

        return self.affected_num()

    def update(self, table, kv_dict, cond_dict):
        """
            更新数据
        :param str|unicode table:   表名
        :param dict kv_dict:        值字典
        :param dict cond_dict:      条件字典
        :return: 受影响的行数
        :rtype: int
        """
        assert isinstance(kv_dict, dict), 'The kv_dict must be a dict.'
        key_str, value_list = self._process_cond_dict(cond_dict)
        kv_k = []
        kv_v = []
        for key, value in kv_dict.items():
            if value is None or value == 'NULL':
                kv_k.append(str(key) + ' = NULL')
            elif isinstance(value, (int, long, float,)):
                kv_k.append('{key} = %s '.format(key=str(key)))
                kv_v.append(str(value * 1))
            else:
                kv_k.append('{key} = %s '.format(key=str(key)))
                kv_v.append(str(value))
        kv = ' , '.join(kv_k)
        sql = 'UPDATE {table} SET {columns} WHERE {condition}'.format(table=table, columns=kv, condition=key_str)
        kv_v += value_list
        self._execute(sql, kv_v, change_db=True)

        return self.affected_num()

    def delete(self, table, cond_dict=None):
        """
            更新数据
        :param str|unicode table:   表名
        :param dict cond_dict:      条件字典
        :return: 受影响的行数
        :rtype: int
        """
        assert cond_dict, 'The delete condition should not be null'
        key_str, value_list = self._process_cond_dict(cond_dict)
        sql = 'DELETE FROM {table:s} WHERE {condition:s}'.format(table=table, condition=key_str)
        self._execute(sql, value_list, change_db=True)
        return self.affected_num()

    def is_auto_commit(self):
        """
            查询当前是否自动提交
        :return: 是否自动提交
        :rtype: bool
        """
        return self._auto_commit

    def auto_commit(self, flag):
        """
            设置当前是否自动提交
        :param bool flag: 自动提交的标志
        :rtype: None
        """
        if self._show_sql:
            print('auto_commit ----> {flag} '.format(flag=flag), file=sys.stdout)
        self._auto_commit = flag

    def rollback(self):
        """
            回滚当前操作
        :rtype: None
        """
        if self._show_sql:
            print('SQL: rollback', file=sys.stdout)
        self._conn.rollback()

    def affected_num(self):
        """
            得到当前操作所影响的行数
        :rtype: int
        """
        return self._cursor.rowcount

    def close(self):
        """
            关闭当前链接
        :rtype: None
        """
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def commit(self):
        """
            提交当前操作
        :rtype: None
        """
        if self._show_sql:
            print('SQL: commit', file=sys.stdout)
        self._conn.commit()

    _level = {
        'debug': True,
        'run': False,
    }

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 当因为错误而退出时,尝试进行回滚
        if exc_type:
            self.rollback()
        self.close()


class DBUtilException(Exception):
    """ 自定义的异常类 """
    pass
