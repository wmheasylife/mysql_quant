# mysql_quant

## 1，更新股票日线数据到本地数据库
### 文件
```shell
{dir}/download_data_stock.py
```
### 功能说明
运行此脚本，每天从baostock 下载文件到本地mysql 数据库。
支持断点下载。
### 运行方式
```python
python3 download_data_stock.py
```

### 依赖文件
```
baostock_date_data.csv
```
### 依赖环境
需要在本地构建mysql数据库，并且为数据库添加默认root 用户，root用户密码为 Meta#2023


# 文件说明
* .gitignore git工程通用文件，忽略被写入.gitignore文件的git工程目录下的文件或者文件夹
* baostock_date_data.csv   baostock 平台给出日线数据的字段信息
* minute_data.csv  baostock 平台给出分钟级别数据的字段信息
* week_month_data.csv  baostock 平台给出周线、月线数据的字段信息
* download_data_stock.py 更新股票日线数据到本地数据库脚本。可以在需要的时候更新本地数据库


# 待更新

