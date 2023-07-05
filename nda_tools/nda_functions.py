# Libraries

from sys import displayhook
import pandas as pd
import numpy as np
from datetime import timedelta
import re
from . import nda_version_8_0


def _validator_cycle(df):
    '''
    To validate the data generated for CycleWise.

    Index(['Cycle_Index', 'Onset_Date', 'End_Date', 'Chg_Cap_(Ah)',
       'DChg_Cap_(Ah)', 'Chg_Energy_(Wh)', 'DChg_Energy_(Wh)',
       'Chg_Time(hh:mm:ss)', 'DChg_Time(hh:mm:ss)', 'Chg_Onset_Volt_(V)',
       'DChg_Oneset_Volt_(V)', 'End_of_Chg_Volt_(V)', 'End_of_DChg_Volt_(V)',
       'Chg_Oneset_Current_(A)', 'DChg_Oneset_Curent_(A)',
       'End_of_Chg_Current_(A)', 'End_of_DChg_Current_(A)', 'DCIR(mΩ)'],
       dtype='object')
    '''
    for id, i in df.iterrows():
        if (pd.to_datetime(i['End_Date']) > pd.to_datetime(i['Onset_Date'])):
            raise ValueError('Date error with cycle number '+str(i['Cycle_Index']))
        if (i["Cycle_Index"] < 1):
            raise ValueError('Cycle Index out of range; please check data.')
        if (i['Chg_Onset_Volt_(V)'] < 1.5 or i['DChg_Onset_Volt_(V)'] < 1.5):
            raise ValueError('Voltage value too low in cycle number '+str(i['Cycle_Index']))
        if (i['DCIR(mΩ)'] < 0):
            raise ValueError('DCIR is negative in cycle number '+str(i['Cycle_Index']))

def get_barcode(nda):
    '''
    returns barcode for said nda file
    '''
    if (nda.split('.')[-1] != 'nda'):
        raise ValueError("File passed in function is not an nda file")
    return nda_version_8_0.get_barcode(nda)


def get_start_time(nda):
    '''
    returns start time for the passed nda file
    '''
    if (nda.split('.')[-1] != 'nda'):
        raise ValueError("File passed in function is not an nda file")
    return nda_version_8_0.get_st_time(nda)


def get_process_name(nda):
    '''
    returns recipe name for said NDA file
    '''
    if (nda.split('.')[-1] != 'nda'):
        raise ValueError("File passed in function is not an nda file")
    return nda_version_8_0.get_process_name(nda)


def records(nda, rename=False):
    '''
    returns a Dataframe record-wise for the nda file
    '''
    if (nda.split('.')[-1] != 'nda'):
        raise ValueError("File passed in function is not an nda file")
    df = nda_version_8_0.nda_in_df_out(nda)
    rec_columns = {
        'record_ID': 'DataPoint',
        'cycle': 'Cycle Index',
        'step_ID': 'Step Index',
        'step_name': 'Step Type',
        'time_in_step': 'Time',
        'voltage_V': 'Voltage(V)',
        'current_mA': 'Current(A)',
        'capacity_mAh': 'Capacity(Ah)',
        'energy_mWh': 'Energy(Wh)',
        'timestamp': 'Date',
        'Validated': 'Validated'
        }
    if (rename == True):
        df['current_mA'] = df['current_mA'].div(1000)
        df['capacity_mAh'] = df['capacity_mAh'].div(1000)
        df['energy_mWh'] = df['energy_mWh'].div(1000)
        df.rename(columns = rec_columns)
    return df



def cycle(df):  #! Function to group the data cycle-wise
    '''
    When passed an nda file or the records data,
    it returns Cycle-wise data identical to the
    cycle sheet in the excel file of the test.
    '''
    try:
        if type(df) != type(pd.DataFrame()):
            df = nda_version_8_0.nda_in_df_out(df)
    except Exception:
        raise ValueError('Arguement pushed into the function is neither a DataFrame nor a path to an nda file')

    temp_list=[]
    complete_list = []
    

    # df['prev_cur'] = df['current_mA'].shift(periods=1)
    # df['prev_vol']=df['voltage_V'].shift(periods=1)
    # df['DCIR']=-1
    # df.loc[((df['prev_cur']==0)&(df['current_mA']!=0)),'DCIR']=abs((df['voltage_V']-df['prev_vol'])/(df['current_mA']-df['prev_cur']))*1000
    # df.drop(columns=['prev_cur','prev_vol'],inplace=True)
    
    chg_temp = 'CCCV_Chg' #default values
    dchg_temp = "CC_Dchg"    
    step_col=list(df['step_name'].unique()[1:])
    for col in step_col:
        if re.search('chg',col,re.IGNORECASE):
            chg_temp=col
        if re.search('dchg',col,re.IGNORECASE):
            dchg_temp=col                           


    for i in range(min(df['cycle']),max(df['cycle']+1)):                         
        # df2=df.groupby('cycle').get_group(i)

        df2 = df[df['cycle']==i]
        cycle=i                                                                                                                                                                           

        starting_date=df2['timestamp'].head(1).values[0]
        end_date=df2['timestamp'].tail(1).values[0]
        
        df_chg=df2[(df2['step_name']==chg_temp)]
        df_dchg=df2[(df2['step_name']==dchg_temp)]

        charging_capacity=max(df_chg['capacity_mAh'],default=-1)
        discharging_capacity=max(df_dchg['capacity_mAh'],default=-1)

        charging_energy=max(df_chg['energy_mWh'],default=-1)
        discharging_energy=max(df_dchg['energy_mWh'],default=-1)

        chg_starting_volt=df_chg['voltage_V'].head(1).values[0]
        chg_ending_volt=df_chg['voltage_V'].tail(1).values[0]
        dchg_starting_volt=df_dchg['voltage_V'].head(1).values[0]
        dchg_ending_volt=df_dchg['voltage_V'].tail(1).values[0]

        chg_starting_current=df_chg['current_mA'].head(1).values[0]
        chg_ending_current=df_chg['current_mA'].tail(1).values[0]
        dchg_starting_current=df_dchg['current_mA'].head(1).values[0]
        dchg_ending_current=df_dchg['current_mA'].tail(1).values[0]

        charging_time=timedelta(seconds=list(df_chg['time_in_step'])[-1])
        discharging_time=timedelta(seconds=list(df_dchg['time_in_step'])[-1])

        dcir_cyl=df2.loc[df2['DCIR']>0,'DCIR']
    
        DCIR_avg=dcir_cyl.mean()     

        temp_list=[
                    cycle,
                    starting_date,
                    end_date,
                    charging_capacity,
                    discharging_capacity,
                    charging_energy,
                    discharging_energy,
                    charging_time,
                    discharging_time,
                    chg_starting_volt,
                    dchg_starting_volt,
                    chg_ending_volt,
                    dchg_ending_volt,
                    chg_starting_current,
                    dchg_starting_current,
                    chg_ending_current,
                    dchg_ending_current,
                    DCIR_avg
                    ]
        
        complete_list.append(temp_list)
    df3=pd.DataFrame(complete_list,columns=['Cycle Index','Onset Date','End Date','Chg. Cap.(Ah)','DChg. Cap.(Ah)',
                                                    'Chg. Energy(Wh)','DChg. Energy_(Wh)','Chg_Time(hh:mm:ss)','DChg_Time(hh:mm:ss)','Chg_Onset_Volt_(V)',
                                                    'DChg_Oneset_Volt_(V)','End_of_Chg_Volt_(V)','End_of_DChg_Volt_(V)','Chg_Oneset_Current_(A)','DChg_Oneset_Curent_(A)',
                                                    'End_of_Chg_Current_(A)','End_of_DChg_Current_(A)','DCIR(mΩ)'])
    return df3


def _validator_step(df) :   

    for id,i in df.iterrows():
        if(pd.to_datetime(i['End Date'])>pd.to_datetime(i['Onset Date'])):
            raise ValueError('Date error with cycle number '+str(i['Cycle_Index']))
        if(i["Cycle_Index"]<1):
            raise ValueError('Cycle Index out of range; please check data.')
        if(i['Chg_Onset_Volt_(V)']<1.5 or i['DChg_Onset_Volt_(V)']<1.5):
            raise ValueError('Voltage value too low in cycle number '+str(i['Cycle_Index']))
        if(i['DCIR(mΩ)']<0):
            raise ValueError('DCIR is negative in cycle number '+str(i['Cycle_Index']))
        # if


def step(df):  #! Function to group the data step-wise
    '''
    When passed an nda file or the records data,
    it returns Step-wise data identical to the
    cycle sheet in the excel file of the test.
    '''
    try:
        if type(df)!=type(pd.DataFrame()):
            df=nda_version_8_0.nda_in_df_out(df)
    except:
        raise ValueError('Arguement pushed into the function is neither a DataFrame nor a path to an nda file')
    # print(df)
    # df['current_mA']=df['current_mA'].div(1000)
    # df['capacity_mAh']=df['capacity_mAh'].div(1000)
    # df['energy_mWh']=df['energy_mWh'].div(1000)
    rec_columns = [
    'record_ID', 'cycle' ,'step_ID','step_name', 'time_in_step', 'voltage_V',
    'current_mA', 'capacity_mAh','energy_mWh','timestamp','Validated','DCIR']
    if(list(df.keys())!=rec_columns):
        raise ValueError ('DataFrame passed ')
        
    
    
    df['prev_cur']=df['current_mA'].shift(periods=1)
    df['prev_vol']=df['voltage_V'].shift(periods=1)
    df['prev_step']=df['step_ID'].shift(periods=1)


    df['DCIR']=-1
    df.loc[((df['prev_cur']==0)&(df['current_mA']!=0)&(df['prev_step']!=df['step_ID'])),'DCIR']=abs((df['voltage_V']-df['prev_vol'])/(df['current_mA']-df['prev_cur']))*1000
    df.drop(columns=['prev_cur','prev_vol','prev_step'],inplace=True)

    temp_list=[]
    complete_list=[]

    for i in range(min(df['step_ID']),max(df['step_ID'])+1):
        # print(i)
        # df2=df.groupby('step_ID').get_group(i)
        df2=df[df['step_ID']==i]
        DCIR=list(df2['DCIR'])[0]
        Starting_Volt=list(df2['voltage_V'])[0]
        End_Voltage=list(df2['voltage_V'])[-1]
        Starting_current=list(df2['current_mA'])[0]
        End_Current=list(df2['current_mA'])[-1]
        Capacity=list(df2['capacity_mAh'])[-1]
        Energy=list(df2['energy_mWh'])[-1]
        Starting_Date=list(df2['timestamp'])[0]
        End_Date=list(df2['timestamp'])[-1]
        Step_Time=timedelta(seconds=list(df2['time_in_step'])[-1])
        step_ID=i
        Maximum_voltage=max(list(df2['voltage_V']))
        Minimum_voltage=min(list(df2['voltage_V']))
        Step_Type=list(df2['step_name'])[0]
        Cycle_Index=list(df2['cycle'])[0]

        temp_list= [
            Cycle_Index,
            step_ID,
            Step_Type,
            str(Step_Time),
            Starting_Date,
            End_Date,
            Capacity/1000,
            Energy/1000,
            Starting_Volt,
            End_Voltage,
            Starting_current/1000,
            End_Current/1000,
            Maximum_voltage,
            Minimum_voltage,
            DCIR,
        ]
        complete_list.append(temp_list)
        
    col_list = ['Cycle Index',
                'Step Number',
                'Step Type',
                'Step Time',
                'Oneset Date',
                'End Date',
                'Capacity(Ah)',
                'Energy(Wh)',
                'Oneset Volt.(V)',
                'End Volt.(V)',
                'Starting current(A)',
                'Termination current(A)',
                'Max Volt.(V)',
                'Min Volt(V)',
                'DCIR(mΩ)']

    df=pd.DataFrame(complete_list,columns=col_list)

    dict_dtypes={
        'Step_Number':np.int64,
        'Step_Type':object,
        'Capacity(Ah)':np.float64,
        'Energy(Wh)':np.float64,
        'Starting_Voltage(V)':np.float64,
        'End_Voltage(V)':np.float64,
        'DCIR':np.float64,
        'Starting_current(A)':np.float64,
        'End_Current(A)':np.float64,
        'Maximum_voltage(V)':np.float64,
        'Minimum_voltage(V)':np.float64
    }

    # df=df.astype(dict_dtypes)

    return df

# Function to find distinct-recipes

def recipe(df):

    try:
        if type(df)!=type(pd.DataFrame()):
            df=nda_version_8_0.nda_in_df_out(df)
    except:
        raise ValueError('Arguement pushed into the function is neither a DataFrame nor a path to an nda file')
    
    dict_voltage={}
    dict_recipe={}
    dict_rest={}
    dict_current={}
    dict_step={}
    dict_cycle={}
    recipe_cycle=[]
    dict_stepid={}
    dict_cutoff_curr={}
    dict_cutoff_vol={}
    chg_temp = ''
    dchg_temp = ''
    step_col=list(df['step_name'].unique()[1:])
    for col in step_col:
        if re.search('_chg',col,re.IGNORECASE):
            chg_temp=col
        if re.search('_dchg',col,re.IGNORECASE):
            dchg_temp=col       

    for i in range(min(df['cycle']),max(df['cycle'])):
        #For non-validated files
        #if(i==7):
            #continue
        # df2=df.groupby('cycle').get_group(i)
        df2=df[df['cycle']==i]
        recipe_cycle.append(i)
        dict_recipe[i]=[]
        dict_rest[i]=[]
        dict_voltage[i]=[]
        dict_current[i]=[]
        dict_step[i]=[]
        dict_cycle[i]=[]
        dict_stepid[i]=[]
        dict_cutoff_curr[i]=[]
        dict_cutoff_vol[i]=[]
        for j in range(min(df2['step_ID']),max(df2['step_ID'])+1):
            df3=df2[df2['step_ID']==j]
            dict_recipe[i].append(df3['step_name'].iloc[0])

            dict_step[i].append(j)

            dict_cycle[i].append(i)

            dict_stepid[i].append(j)

            if(df3['step_name'].iloc[0]=='Rest'):
                dict_rest[i].append(str(timedelta(seconds=list(df3['time_in_step'])[-1])))
            else:
                dict_rest[i].append(np.nan)

            if(df3['step_name'].iloc[0]==chg_temp):
                dict_voltage[i].append(round(df3['voltage_V'].max(),2))
                dict_cutoff_vol[i].append(np.nan)
            elif(df3['step_name'].iloc[0]==dchg_temp):
                dict_voltage[i].append(round(df3['voltage_V'].min(),2))
                dict_cutoff_vol[i].append(round(list(df3['voltage_V'])[-1],2))
            else:
                dict_voltage[i].append(np.nan)
                dict_cutoff_vol[i].append(np.nan)


            if(df3['step_name'].iloc[0]==chg_temp):
                dict_current[i].append(round(df3['current_mA'].iloc[0]/1000,2))
                dict_cutoff_curr[i].append(round(list(df3['current_mA'])[-1]/1000,2))
            elif(df3['step_name'].iloc[0]==dchg_temp):
                dict_current[i].append(round(df3['current_mA'].iloc[0]/1000,2))
                dict_cutoff_curr[i].append(np.nan)
            else:
                dict_current[i].append(np.nan)
                dict_cutoff_curr[i].append(np.nan) 

    recipe=[]
    rest=[]
    voltage=[]
    current=[]
    cycle=[]
    stepid=[]
    cutoff_curr=[]
    cutoff_vol=[]

    for i in recipe_cycle:
        for j in range(len(dict_recipe[i])):
            recipe.append(dict_recipe[i][j])
            rest.append(dict_rest[i][j])
            voltage.append(dict_voltage[i][j])
            current.append(dict_current[i][j])
            cycle.append(dict_cycle[i][j])
            stepid.append(dict_stepid[i][j])  
            cutoff_curr.append(dict_cutoff_curr[i][j])
            cutoff_vol.append(dict_cutoff_vol[i][j])                     

    df_recipe=pd.DataFrame(zip(np.array(stepid).reshape(-1,1),
                           np.array(cycle).reshape(-1,1),
                           np.array(recipe).reshape(-1,1),
                           np.array(voltage).reshape(-1,1),
                           np.array(current).reshape(-1,1),
                           np.array(rest).reshape(-1,1),
                           np.array(cutoff_curr).reshape(-1,1),
                           np.array(cutoff_vol).reshape(-1,1)),columns=['Step_Id','Cycle','Step_Name','Voltage','Current','Rest','Cutoff_current','Cutoff_voltage'])                      
    
    for col in df_recipe.columns:
        df_recipe[col]=df_recipe[col].apply(lambda x : x[0]) 
        
    df_recipe.replace(np.nan,r' ',regex=True,inplace=True)
    df_recipe.replace(r'nan',r' ',regex=True,inplace=True)

    recipe_unmatch=[1]
    for i in range(len(recipe_cycle)-1):
        
        df_temp1=df_recipe[df_recipe['Cycle']==recipe_cycle[i]]
        
        df_temp2=df_recipe[df_recipe['Cycle']==recipe_cycle[i+1]]
        df_temp1=df_temp1.drop(['Cycle','Step_Id'],axis=1)
        df_temp2=df_temp2.drop(['Cycle','Step_Id'],axis=1)
        if(df_temp1.reset_index(drop=True).equals(df_temp2.reset_index(drop=True))):
            continue
        else:
            recipe_unmatch.append(recipe_cycle[i]) 
    recipe_unmatch.append(df_recipe['Cycle'].max())           
    recipe_unmatch=sorted(list(set(recipe_unmatch)))

    dict={}
    for i in range(len(recipe_unmatch)-1):
        dict[recipe_unmatch[i]]=[]
        #df1=df_recipe.groupby('Cycle').get_group(recipe_unmatch[i]).reset_index(drop=True)
        df1=df_recipe[df_recipe['Cycle']==recipe_unmatch[i]].reset_index(drop=True)
        df1=df1.drop(['Cycle','Step_Id'],axis=1)
        for j in range(i+1,len(recipe_unmatch)):
            #df2=df_recipe.groupby('Cycle').get_group(recipe_unmatch[j]).reset_index(drop=True)
            df2=df_recipe[df_recipe['Cycle']==recipe_unmatch[j]].reset_index(drop=True)
            df2=df2.drop(['Cycle','Step_Id'],axis=1)
            if(df1.equals(df2)):
                dict[recipe_unmatch[i]].append(recipe_unmatch[j]) 

    for i in dict:
        for j in dict[i]:
            if j in recipe_unmatch:
                recipe_unmatch.remove(j)
           
    arr=[]
    dict_={}
    k=0
    print(recipe_unmatch)
    for i in range(len(recipe_unmatch)):
        k+=1   
        # df_temp=df_recipe.groupby('Cycle').get_group(recipe_unmatch[i])
        df_temp=df_recipe[df_recipe['Cycle']==recipe_unmatch[i]]
        
        df_temp=df_temp.drop(['Cycle','Step_Id'],axis=1).reset_index(drop=True)
        arr.append(df_temp)
        print('Recipe-{k}'.format(k=k))
        dict_['Recipe-{k}'.format(k=k)]=df_temp
        displayhook(df_temp)
       
