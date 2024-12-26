def get_rth_df(self,data,n): #收益率
        data['pv1']=data['pv1'] + np.random.normal(0, 1e-10, size=len(data))
        data['decile'] = pd.qcut(data['pv1'], 100, labels=range(1, 101))
        data_lag = data.copy()
        data_lag.rename({'price':'lagged_price'},axis=1,inplace=True)
        data_lag['time_sec']= data_lag['time_sec']-n
        data.sort_values(by=['time_sec','code'],inplace=True)
        data_lag.sort_values(by=['time_sec','code'],inplace=True)
        tmp_result=pd.merge_asof(data,data_lag,on='time_sec',by='code',direction='forward')
        tmp_result.dropna(inplace=True)
        tmp_result=tmp_result[['code', 'time_sec', 'pv1_x','price', 'lagged_price','decile_x']]
        tmp_result.rename({'pv1_x':'pv1','decile_x':'decile'},axis=1,inplace=True)
        tmp_result['rth']=(tmp_result['lagged_price']-tmp_result['price'])/tmp_result['price']
        return tmp_result

def get_long_short_return(self,data): #多空收益
    long_data=data[data['decile']==100]
    short_data=data[data['decile']==1]
    rth=(long_data['rth'].mean()-short_data['rth'].mean())*10000
    return rth

def get_no_mean_ic(self,data): #不减均值ic
    rth=data['rth']
    pv1=data['pv1']
    numerator=(rth*pv1).sum()
    denominator=(((pv1**2).sum())**0.5) * (((rth**2).sum())**0.5)
    return numerator/denominator
def get_imbalance(self,data): #多空imbalance
    up=data['pv1'].quantile(0.99)
    down=data['pv1'].quantile(0.01)
    imbalance=abs(down)/abs(up)
    return imbalance
