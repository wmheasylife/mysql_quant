"""
下载股票日K线数据
1, 判断对应股票更新到哪一天，接着当时的日期更新
"""
import baostock as bs
import datetime
import sys
import numpy as np
import pandas as pd
import multiprocessing
import pymysql
import logging
import sqlalchemy
import math


def create_mysql_engine():
    """
    创建数据库引擎对象
    :return: 新创建的数据库引擎对象
    """

    # 引擎参数信息
    host = 'localhost'
    user = 'root'
    passwd = 'Web#rndcl75'
    port = 3306
    db = 'db_quant'

    # 创建数据库引擎对象
    db_engine = pymysql.connect(host=host, user=user, password=passwd, port=3306)

    # 如果不存在数据库db_quant则创建
    db_engine.cursor().execute("create database if not exists {0} ".format(db))

    # 创建连接数据库db_quant的引擎对象
    db_engine = pymysql.connect(host=host, user=user, password=passwd, database=db,
                                cursorclass=pymysql.cursors.DictCursor,
                                port=port)

    # 返回引擎对象
    return db_engine

# BaoStock日线数据字段
g_baostock_date_data_fields = 'date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST'


class UpdateStockDailyData:
    def __init__(self) -> None:
        self.stock_codes = list()
        self.stock_newest_data_date = dict()
        self.default_oldest_data_date =  '2009-02-28' # 查询Date数据时，不包含 from_date, 最早数据为2009-03-01
        self.mysql_quant_engine = create_mysql_engine()
        self.column_data_type = dict()

    def get_stock_codes(self, date=None):
        """
        获取指定日期的A股代码列表
        若参数date为空，则返回最近1个交易日的A股代码列表
        若参数date不为空，且为交易日，则返回date当日的A股代码列表
        若参数date不为空，但不为交易日，则打印提示非交易日信息，程序退出
        :param date: 日期
        :return: A股代码的列表
        """

        # 登录baostock
        bs.login()

        # 从BaoStock查询股票数据
        stock_df = bs.query_all_stock(date).get_data()

        # 如果获取数据长度为0，表示日期date非交易日
        if 0 == len(stock_df):

            # 如果设置了参数date，则打印信息提示date为非交易日
            if date is not None:
                logging.info('当前选择日期为非交易日或尚无交易数据，请设置date为历史某交易日日期')
                return False

            # 未设置参数date，则向历史查找最近的交易日，当获取股票数据长度非0时，即找到最近交易日
            delta = 1
            today = datetime.date.today()
            while 0 == len(stock_df):
                stock_df = bs.query_all_stock(today - datetime.timedelta(days=delta)).get_data()
                delta += 1

        # 注销登录
        bs.logout()

        # 筛选股票数据，上证和深证股票代码在sh.600000与sz.39900之间
        stock_df = stock_df[(stock_df['code'] >= 'sh.600000') & (stock_df['code'] < 'sz.399000')]

        # 返回股票列表
        self.stock_codes = stock_df['code'].tolist()
        # self.stock_codes = ['sh.600110']

        logging.info('下载最近的股票列表成功! 股票数量为 %s' %(len(self.stock_codes)))
        return True

    def get_stock_newest_date(self, stock_codes=list()):
        """
        从数据库中获取最新股票的日线数据日期。如果对应的股票不存在最新的日线数据，
        那么返回(字符串类型的)2009-03-01
        """
        cursor = self.mysql_quant_engine.cursor()
        ret = cursor.execute('select table_name from information_schema.tables where table_schema="db_quant"')
        table_name_result = cursor.fetchall()
        table_list = list()
        for table_name in table_name_result:
            table_list.append(table_name['TABLE_NAME'])
        logging.debug('table_list is :' + str(table_list))
        logging.basicConfig()
        temp_df = pd.read_csv('baostock_date_data.csv')
        for index,column_info in temp_df.iterrows():
            # create_table_sql += column_info['参数名称'] + ' ' + column_info['data_type'] + ','
            self.column_data_type[column_info['参数名称']] = column_info['data_type']

        for stock_code in stock_codes:
            table_name = '{}_{}'.format(stock_code[3:], stock_code[:2])
            if table_name in table_list:
                get_stock_date_sql = 'select date from ' + table_name + ' order by date desc limit 1'
                logging.debug(get_stock_date_sql)
                cursor.execute(get_stock_date_sql)
                ret = cursor.fetchall()
                if len(ret) == 0:
                    self.stock_newest_data_date[stock_code] = self.default_oldest_data_date
                else:
                    # 找到最新的数据
                    for item in ret:
                       self.stock_newest_data_date[stock_code] = (item['date'] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                continue
            else:
                self.stock_newest_data_date[stock_code] = self.default_oldest_data_date
                create_table_sql= 'create table %s (' % (table_name)
                temp_df = pd.read_csv('baostock_date_data.csv')
                for col_name,data_type in self.column_data_type.items():
                    create_table_sql += col_name + ' ' + data_type + ','

                create_table_sql += 'index(date)'
                create_table_sql += ')'
                logging.info('will execute create table sql with sql [' + create_table_sql + ']')
                cursor.execute(create_table_sql)
        logging.debug('self.stock_newest_data_date is : ' + str(self.stock_newest_data_date))
        cursor.close()
        self.mysql_quant_engine.commit()


    def create_data(self, stock_codes, adjustflag='2'):
        """
        下载指定日期内，指定股票的日线数据
        :param stock_codes: 待下载数据的股票代码
        :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
        :return: None
        """

        logging.info('调用 create_data 函数，传入了 ' + str(len(stock_codes)) + ' 个股票!')


        # 创建数据库引擎对象
        bs.login()


        # 下载股票循环
        cursor = self.mysql_quant_engine.cursor()
        to_date = datetime.datetime.today().strftime('%Y-%m-%d')
        # to_date = '2022-10-30'
        all_stock_num = len(stock_codes)
        for i in range(all_stock_num):
            stock_code = stock_codes[i]
            from_date = self.stock_newest_data_date[stock_code]
            today_str = datetime.datetime.today().strftime('%Y-%m-%d')
            if from_date > today_str:
                logging.info('[{}/{}] {} 的日线数据最新日期为{}，无需更新!'.format(i + 1, all_stock_num, stock_code, from_date))
                continue
            logging.info('[{}/{}] 正在更新{}日线数据 . from {} ~ {}'.format(i + 1, all_stock_num, stock_code, from_date, to_date))

            # 登录BaoStock
            # # 能否放在外部??
            # bs.login()

            # 下载日线数据
            out_df = bs.query_history_k_data_plus(stock_code, g_baostock_date_data_fields,
                                                  start_date=from_date, end_date=to_date,
                                                  frequency='d', adjustflag=adjustflag).get_data()


            code_column_list = [stock_code for i in range(len(out_df.index))]
            out_df['code'] = code_column_list

            # 无数据时，返回
            if not out_df.shape[0]:
                continue
            # 写入数据库
            table_name = '{}_{}'.format(stock_code[3:], stock_code[:2])
            # out_df.to_csv('stock_date_data/' + table_name + '.csv')
            # 将数值数据转为float型，便于后续处理
            # logging.debug('out_df columns is : ' + ','.join(out_df.columns))
            # convert_list = ['open', 'high', 'low', 'close', 'preclose', 'amount', 'turn', 'pctChg','peTTM', 'psTTM', 'pcfNcfTTM', 'pbMRQ']
            # # print()
            # out_df[convert_list] = out_df[convert_list].replace('', '0').astype(float)

            # convert_list = ['open', 'high', 'low', 'close', 'preclose', 'amount', 'turn', 'pctChg','psTTM','pbMRQ'
            # ]
            convert_list = ['open', 'high', 'low', 'close', 'preclose', 'amount', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'psTTM',  'pcfNcfTTM']
            out_df[convert_list] = out_df[convert_list].replace('', np.nan).astype(float)
            convert_list = ['volume']
            out_df[convert_list] = out_df[convert_list].replace('', 0).fillna(0).astype(int)

            # # 为元数据添加停牌列
            # if out_df.shape[0]:
            #     # out_df = out_df[(out_df['volume'] != '0') & (out_df['volume'] != '')]
            #     # 通过Volude 判断是否停牌
            #     out_df['is_suspend'] = (out_df['volume'] != '0') | (out_df['volume'] != '')

            # 删除重复数据
            out_df.drop_duplicates(['date'], inplace=True)

            # 重置索引
            out_df.reset_index(drop=True, inplace=True)

 
            # 查询到底数据dump到数据库!
            all_columns = out_df.columns
            batch_insert_table_sql = 'insert into ' + table_name + '(' + ','.join(all_columns) + ') value ('
            batch_insert_table_sql += ','.join(['%s' for i in range(len(all_columns))]) + ')'
            stock_date_data_list = list()
			# logging.debug('batch_insert_table_sql is : ' + batch_insert_table_sql)
            for row_num,row in out_df.iterrows():
                temp_list = list()
                # logging.debug('[' + str(row_num + 1) + '] ' + str(row))
                for col_name in all_columns:
                    if self.column_data_type[col_name] == 'double' and math.isnan(row[col_name]):
                        temp_list.append(None)
                        continue
                    temp_list.append(row[col_name])

                stock_date_data_list.append(temp_list)
            cursor.executemany(batch_insert_table_sql, stock_date_data_list)
            self.mysql_quant_engine.commit()
        cursor.close()
        bs.logout()
        # self.mysql_quant_engine.commit()


    def get_code_group(self, process_num, stock_codes):
        """
        获取代码分组，用于多进程计算，每个进程处理一组股票

        :param process_num: 进程数
        :param stock_codes: 待处理的股票代码
        :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
        """

        # 创建空的分组
        code_group = [[] for i in range(process_num)]

        # 按余数为每个分组分配股票
        for index, code in enumerate(stock_codes):
            code_group[index % process_num].append(code)


        logging.debug('code group number is : ' + str(len(code_group)))
        return code_group


    def multiprocessing_func(self, func, args):
        """
        多进程调用函数

        :param func: 函数名
        :param args: func的参数，类型为元组，第0个元素为进程数，第1个元素为股票代码列表
        :return: 包含各子进程返回对象的列表
        """

        logging.info('call multiprocessing_func')
        print(func)

        results = list()
        # 创建进程池
        with multiprocessing.Pool(processes=args[0]) as pool:
            # 多进程拉取股票时间
            for codes in self.get_code_group(args[0], args[1]):
                results.append(pool.apply_async(func, args=(codes, *args[2:],)))

            # 阻止后续任务提交到进程池
            pool.close()

            # 等待所有进程结束
            pool.join()



    def create_data_mp(self, stock_codes, process_num=5, adjustflag='2'):
        """
        使用多进程创建指定日期内，指定股票的日线数据，计算扩展因子

        :param stock_codes: 待创建数据的股票代码
        :param process_num: 进程数
        :param from_date: 日线开始日期
        :param to_date: 日线结束日期
        :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
        :return: None
        """

        self.multiprocessing_func(UpdateStockDailyData.create_data, (process_num, stock_codes, adjustflag,))

    def do_update(self):
        self.get_stock_codes()
        self.get_stock_newest_date(self.stock_codes)
        self.create_data(self.stock_codes)
        # self.create_data_mp(self.stock_codes)


if __name__ == '__main__':
    LOG_FORMAT = '%(asctime)s %(levelname)s [%(filename)s:%(lineno)s] %(message)s'
    logging.basicConfig(filename='update_stock.log', level=logging.DEBUG, format=LOG_FORMAT)
	# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.info('Finish init logging!')
    update_lock_stock = UpdateStockDailyData()
    update_lock_stock.do_update()
    # stock_codes = get_stock_codes()
    # create_data_mp(stock_codes[:5])
