import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
from pyarrow import fs
from itertools import combinations
import multiprocessing
from multiprocessing import Manager, Pool
from tqdm import tqdm
import warnings
import seaborn as sns
import matplotlib.pyplot as plt
warnings.filterwarnings('ignore')
import re
from pathlib import Path
import datetime

def get_data(path):
    path=path
    hdfs = fs.HadoopFileSystem("hdfs://ftxz-hadoop/", user='zli')
    df = pq.read_table(path,filesystem=hdfs).to_pandas()
    return df
def get_data_path(server_path,date): #获取数据路径
    hdfs = fs.HadoopFileSystem("hdfs://ftxz-hadoop/", user='zli')
    try:
        files=[x.path for x in hdfs.get_file_info(fs.FileSelector(f'/user/zli/app/real_backup/{server_path}/fut_signal_real/{date}', recursive=False))]
        pattern = r'fut_signal_\d{4}_\d{8}\.parquet' #设置正则表达式规则
        data_path = [file for file in files if re.search(pattern, file)]
        return data_path
    except Exception as e:
        return None

def get_data_dict(server_01,server_02,date):
    data_dict={}
    list_signal=[]
    path_ls=get_data_path(server_01,date)
    if path_ls==None:
        print(f"{date}的{server_01}没有数据")
    else:
        for path in path_ls:
            pattern = r'fut_signal_(\d{4})_\d{8}\.parquet' #设置正则表达式规则
            match = re.search(pattern, path)
            if match:
                signal_id = int(match.group(1))
                data=get_data(path)
                if data.empty:
                    print(f'10服务器{path}信号{signal_id}数据为空')
                    continue
                else:
                    list_signal.append(signal_id) # 提取到的信号编号
            else:
                print("未找到匹配的信号编号")
            data['date']=date
            data.rename(columns={'depth_timesec':'time_sec'},inplace=True)
            data=data[['date','code','time_sec','bid1','ask1','pv1','pv2']]
            data=data.sort_values(by=['date','time_sec','code']).reset_index(drop=True)
            data_dict[signal_id]=data

    path_ls=get_data_path(server_02,date)
    if path_ls==None:
        print(f"{date}的{server_02}没有数据")
    else:
        for path in path_ls:
            pattern = r'fut_signal_(\d{4})_\d{8}\.parquet' #设置正则表达式规则
            match = re.search(pattern, path)
            if match:
                signal_id = int(match.group(1))
                if signal_id in list_signal:
                    continue
                else:
                    data=get_data(path)
                    if data.empty:
                        print(f'08服务器{path}信号{signal_id}数据为空')
                        continue
                    else:
                        list_signal.append(signal_id) # 提取到的信号编号
                    data['date']=date
                    data.rename(columns={'depth_timesec':'time_sec'},inplace=True)
                    data=data[['date','code','time_sec','bid1','ask1','pv1','pv2']]
                    data=data.sort_values(by=['date','time_sec','code']).reset_index(drop=True)
                    data_dict[signal_id]=data
            else:
                print("未找到匹配的信号编号")
    return data_dict,list_signal

def calculate_return(price_numpy, n):
    rt_n = ((  price_numpy[n:] - price_numpy[:len(price_numpy)-n]) / price_numpy[:len(price_numpy)-n]) * 10000
    rt_n_padded = np.pad(rt_n, (0, n), 'constant', constant_values=(np.nan, np.nan))
    return rt_n_padded
def fill_zeros_with_prev(df, column):
    """
    将指定列的0值替换为该列中0之前的最后一个非零值。
    """
    prev_value = None
    for i in range(len(df)):
        if df[column][i] != 0:
            prev_value = df[column][i]
        if df[column][i] == 0 and prev_value is not None:
            df.at[i, column] = prev_value
    return df
def handle_stop_data(df): #处理涨跌停板数据
        tmp_ls=[] #暂时存放临时数据
        code=df['code'].unique()
        for i in code:
            #print(i)
            tmp=df[df['code']==i]
            #print(len(tmp))
            tmp=tmp.sort_values(by=['date','time_sec']).reset_index(drop=True)
            tmp = fill_zeros_with_prev(tmp, 'ask1')
            tmp = fill_zeros_with_prev(tmp, 'bid1')
            #print(len(tmp))
            tmp_ls.append(tmp)
        handled_df=pd.concat(tmp_ls,axis=0)
        return handled_df



#date='20241216'
date=datetime.datetime.now().strftime('%Y%m%d') 
server_01='xtpsh10'
server_02='xtpsh08'
folder_path = Path(f'/data2/share/dja_shared/fut_shared/signal_quantile/{date}')
folder_path.mkdir(parents=True, exist_ok=True)    # 创建文件夹
data_dict,list_signal=get_data_dict(server_01,server_02,date)
if len(data_dict)==0:
    print(f'{date},今日无数据')
else:
    result_ls=[]
    for signal_id in list_signal:
        data=data_dict[signal_id]
        describe_stats = data['pv1'].describe(percentiles=[0.01, 0.02, 0.03, 0.05, 0.10, 0.90, 0.95, 0.97, 0.98, 0.99])
        describe_df= pd.DataFrame(describe_stats)
        describe_df.columns = [signal_id]  # 设置列名
        result_ls.append(describe_df)
    result=pd.concat(result_ls,axis=1)
    result=result.drop(index='count')
    result=result.T
    result['imbalance']=result['1%'].abs()/result['99%'].abs()
    result=result.T
    result=result.fillna(0)
    plt.figure(figsize=(20, 16))
    sns.heatmap(result, annot=True, fmt=".3f", xticklabels=result.columns, yticklabels=result.index)
    plt.title('quantile') 
    plt.tight_layout()
    plt.savefig(f'/data2/share/dja_shared/fut_shared/signal_quantile/{date}/signal_quantile_{date}.png', dpi=300)
print(f'quantile定时完成{date}')