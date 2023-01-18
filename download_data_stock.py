"""
下载股票日K线数据
1, 判断对应股票更新到哪一天，接着当时的日期更新
"""
import baostock as bs
import datetime
import numpy as np
import pandas as pd
import multiprocessing
import pymysql
import logging
import random
import time
import math



def init_logging():
    LOG_FORMAT = '%(asctime)s %(levelname)s %(process)d %(processName)s [%(filename)s:%(lineno)s] %(message)s'
    # logging.basicConfig(filename='update_stock.log', level=logging.DEBUG, format=LOG_FORMAT)
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def create_mysql_engine():
    """
    创建数据库引擎对象
    :return: 新创建的数据库引擎对象
    """

    # 引擎参数信息
    host = 'localhost'
    user = 'root'
    passwd = 'Meta#2023'
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
    def __init__(self, stock_codes, column_data_type, adjustflag='2'):
        # 异步进程处理，这里需要重新初始化一下 logging
        # 
        self.stock_codes = stock_codes
        self.column_data_type = column_data_type
        self.adjustflag = adjustflag
        self.mysql_quant_engine = create_mysql_engine()

    def do_update(self):
        return self.create_data(self.stock_codes)

    def create_data(self, stock_codes):
        """
        下载指定日期内，指定股票的日线数据
        :param stock_codes: 待下载数据的股票代码
        :return: None
        """

        all_stock_num = len(stock_codes)
        logging.info('调用 create_data 函数，传入了 ' + str(len(stock_codes)) + ' 个股票!')
        if all_stock_num == 0:
            return True

        # 登录 baostock
        bs.login()

        # 下载股票循环
        cursor = self.mysql_quant_engine.cursor()
        to_date = datetime.datetime.today().strftime('%Y-%m-%d')
        # to_date = '2022-10-30'

        for i in range(all_stock_num):
            stock_code = stock_codes[i][0] # stock_code
            from_date = stock_codes[i][1] # stock_code new date
            today_str = datetime.datetime.today().strftime('%Y-%m-%d')
            if from_date > today_str:
                logging.info('[{}/{}] {} 的日线数据最新日期为{}，无需更新!'.format(i + 1, all_stock_num, stock_code, from_date))
                continue
            logging.info('[{}/{}] 正在更新 {} 日线数据 from {} ~ {}'.format(i + 1, all_stock_num, stock_code, from_date, to_date))


            # 下载日线数据
            out_df = bs.query_history_k_data_plus(stock_code, g_baostock_date_data_fields,
                                                  start_date=from_date, end_date=to_date,
                                                  frequency='d', adjustflag=self.adjustflag).get_data()

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
        return True

class GenerateStockCodeGroup:

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

    def get_stock_newest_date(self):
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

        for stock_code in self.stock_codes:
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
                    today_str = datetime.datetime.today().strftime('%Y-%m-%d')
                    for item in ret:
                        code_next_date_str = (item['date'] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                        if code_next_date_str > today_str:
                            logging.info(stock_code + ' has newest trate data, with next_date_str:' + code_next_date_str)
                            continue
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


    def get_code_group(self, process_num):
        """
        获取代码分组，用于多进程计算，每个进程处理一组股票

        :param process_num: 进程数
        :param stock_codes: 待处理的股票代码
        :return: 分组后的股票代码列表，列表的每个元素为一组股票代码的列表
        """

        # 创建空的分组
        all_stock_number = len(list(self.stock_newest_data_date.keys()))
        code_group = [[] for i in range(all_stock_number)]

        # 按余数为每个分组分配股票
        index = 0
        for stock_code,stock_new_date in self.stock_newest_data_date.items():
            code_group[index % process_num].append((stock_code, stock_new_date))
            index += 1

        logging.debug('code group number is : ' + str(len(code_group)))
        return code_group

    def get_columns_data_type(self):
        return self.column_data_type

def do_one_group_update_stock(i, stock_codes, columns_data_type, adjustflag='2'):
    """
    独立函数，异步更新 stock 日线数据的入口

    :param stock_codes 这个进程需要处理股票的列表，以及股票日线数据的起始日期
    :param clumns_data_type 这个时对应股票列表的数据类型
    :param adjustflag  复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
    """
    init_logging()
    logging.info('will run do_one_group_update_stock')
    # time.sleep(random.randint(1 * 10))
    try:
        update_lock_stock = UpdateStockDailyData(stock_codes, columns_data_type, adjustflag)
        succ = update_lock_stock.do_update()
    except Exception as e:
        logging.exception(e) # 这里，异步进程异常退出时， stdout 看不到任何东西，这里使用logging.exception(e) 输出为log
        succ = False

    return (i, succ)

def callback_update_stock(result):
    logging.info('update stock result is :' + str(result))

def create_data_mp(process_num=5, adjustflag='2'):
    """
    多进程调用函数

    :param funcprocess_num: 期望进程数量，最好和CPU核心数相同
    :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
    """

    logging.info('call multiprocessing_func')

    results = list()
    # 创建进程池
    pool =  multiprocessing.Pool(processes=process_num)
    # 多进程拉取股票时间
    temp_generater = GenerateStockCodeGroup()
    temp_generater.get_stock_codes()
    temp_generater.get_stock_newest_date()
    columns_data_type = temp_generater.get_columns_data_type()
    index = 0
    for codes in temp_generater.get_code_group(process_num):
        logging.info('will apply async process. do_one_group_update_stock' + str(codes[:10]) + ' with index:' + str(index))
        pool.apply_async(do_one_group_update_stock, args=(index, codes, columns_data_type, adjustflag), callback=callback_update_stock) 
    pool.close()
    pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    print("Sub-process(es) done.")



if __name__ == '__main__':
    init_logging()
    logging.info('Finish init logging!')
    create_data_mp(5, '2')