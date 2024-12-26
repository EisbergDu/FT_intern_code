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
def get_quantile_data(data):
    data['decile'] = pd.qcut(data['pv1'], 100, labels=range(1, 101))
    tmp_1=data[data['decile'] == 1]
    tmp_2=data[data['decile'] == 100]
    data=pd.concat([tmp_1,tmp_2],axis=0)
    return data

def process_2_signal_inner_combination(args):
    signal_comb,data_dict=args
    data_1=data_dict[signal_comb[0]]
    data_2=data_dict[signal_comb[1]]
    data_1=get_quantile_data(data_1)
    data_2=get_quantile_data(data_2)
    merged_df = pd.merge(data_1, data_2, on=['time_sec', 'code', 'date'])
    inner_rate=len(merged_df)/(len(data_1)+len(data_2))
    dict_tmp={
        'signal_comb':signal_comb,
        'inner_rate':inner_rate
    }
    return dict_tmp

if __name__ == '__main__':
    date=datetime.datetime.now().strftime('%Y%m%d')  #如果要设置定时任务就用这个
    #date='20241216'
    server_01='xtpsh10'
    server_02='xtpsh08'
    print(f'{date}开始分析')
    folder_path = Path(f'/data2/share/dja_shared/fut_shared/signal_similarity/{date}')
    folder_path.mkdir(parents=True, exist_ok=True)    # 创建文件夹
    data_dict,signal_list=get_data_dict(server_01,server_02,date)
    for key, value in data_dict.copy().items():
        if value.empty:  # 这里的 not 会检查空值（None、空字符串、空列表、空字典等）
            print(f"'{key}' 的值是空的")
            del data_dict[key]
        else:
            continue
    signal_list=list(data_dict.keys())
    signal_combinations = list(combinations(signal_list, 2))
    pool = multiprocessing.Pool(processes=8)  # 可以根据你的CPU核心数调整
    dict_set=list(tqdm(pool.imap(process_2_signal_inner_combination, 
                                 [(comb, data_dict) for comb in signal_combinations]), 
                                 total=len(signal_combinations)))
    pool.close()
    pool.join()
    heatmap_df=pd.DataFrame(index=signal_list,columns=signal_list)
    df = pd.json_normalize(dict_set)
    for i in range(len(df)):
            heatmap_df.loc[df.loc[i,'signal_comb'][0],df.loc[i,'signal_comb'][1]]=df.iloc[i]['inner_rate']
            heatmap_df.loc[df.loc[i,'signal_comb'][1],df.loc[i,'signal_comb'][0]]=df.iloc[i]['inner_rate']
    heatmap_df.fillna(0,inplace=True)
        # 绘制热力图
    plt.figure(figsize=(20, 16))
    sns.heatmap(heatmap_df, annot=True, fmt=".3f", annot_kws={"size": 12},xticklabels=heatmap_df.index, yticklabels=heatmap_df.columns)
    plt.title('qHeatmap of inner percentage') 
    plt.tight_layout()
    output_path = f'/data2/share/dja_shared/fut_shared/signal_similarity/{date}/信号重叠度_{date}.png'
    plt.savefig(output_path, dpi=300)
    plt.close()  
print(f'{date}分析结束')