#!/bin/python3.4

# from audioop import mul
# import sys, getopt
import struct
# import binascii
# import time
import logging
import mmap
import re
import datetime
import pandas as pd
ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

def validate_timegap(df):

    
    """
    #:Check if there is a gap due to electricity cutting off and thus if there are errors there. 
    #:Can check if removing those records makes the dataframe pass the validation    
    """
    
    
    df['prev_step']=df['step_ID'].shift(periods=1)
    
    df['prev_tis']=df['time_in_step'].shift(periods=1)
    df['prev_tstamp']=df['timestamp'].shift(periods=1)
    df['prev_tis']=df['time_in_step']-df['prev_tis']
    df['prev_tstamp']=df['timestamp']-df['prev_tstamp']
    df['prev_tstamp']=df['prev_tstamp'].dt.total_seconds()
    

    #todo prev_tis and prev_tstamp now represent difference. of the time in step and timestamp 
    df.loc[( (abs(df['prev_tis']-df['prev_tstamp'])>5) & ((df['step_ID']==df['prev_step']) & (df['time_in_step']!=0)) ),'Validated']=True
    df.drop(columns=['prev_tis','prev_tstamp','prev_step'],inplace=True)
       
    









def main_validator(df,capacity_nom):
    """
    It checks if the dataframe is valid. 
    
    The function takes in a dataframe and a capacity_nom. 
    
    It checks if the record_ID is monotonically increasing and starts at 1. 
    
    It checks if the cycle starts at 1. 
    
    It checks if the step_ID is monotonically increasing and starts at 1. 
    
    It checks if the voltage is greater than 2. 
    
    It checks if the capacity is greater than 0. 
    
    It checks if the capacity is less than 1500 times the capacity_nom. 
    
    It checks if the current is less than 1600 times the capacity_nom. 
    
    It checks if the process name, start time, and barcode are valid. 
    
    It checks if the barcode is 12 characters long.
    
    Args:
      df: the dataframe to be validated
      capacity_nom: The nominal capacity of the battery.
    
    Returns:
      True if the dataframe is valid, and False if it is not.
    """
  
    validate_timegap(df)
    if(False in df.Validated.values):
        print('Df validation failed. Kindly check.')
        logging.info("This df has failed the basic validation after ignoring the timegaps' records")
        return False


    #!Could return numbers instead of True False to indicate what error we have. And then check for electricity timegap with that error
    df.loc[(df['step_name']=='Rest') & (df['current_mA']==0) & (df['time_in_step']!=0),'Validated']=False 
    if(df['record_ID'].min()!=1) or (not df['record_ID'].is_monotonic_increasing): 
        logging.info("Record_ID error. Min record_ID is: "+str(df['record_ID'].min()))
        return False
    if(df['cycle'].min()!=1): 
        logging.info("Cycle error. Min cycle is: "+str(df['cycle'].min()))
        return False
    if(df['step_ID'].min()!=1 or (not df['step_ID'].is_monotonic_increasing)): 
        logging.info("step_ID error")
        return False
    if((df['voltage_V'].min()<2)and (df[df['step_name']=='SIM'].shape[0]==0)): 
        logging.info("Voltage error. Min voltage is: "+str(df['voltage_V'].min()))
        return False
    if((df['energy_mWh'].min()<0) and (df[df['step_name']=='SIM'].shape[0]==0)):
        logging.info("Negative Energy error. Min energy is: "+str(df['energy_mWh'].min()))
        return False
    
    if((df['capacity_mAh'].min()<0) and (df[df['step_name']=='SIM'].shape[0]==0)):
        logging.info("Negative Capacity error. Min capacity is: "+str(df['capacity_mAh'].min()))
        return False
    
    if(df['capacity_mAh'].max()>(1500*capacity_nom)): 
        logging.info("Max Capacity error. Max capacity is: "+str(df['capacity_mAh'].max()))
        return False
    if(df['current_mA'].max()>(1600*capacity_nom)):
        logging.info("Current error. Max current is: "+str(df['current_mA'].max()))
        return False
    if all(ele in df.keys() for ele in ['Process Name','Start Time','barcode']):    
        if ILLEGAL_CHARACTERS_RE.search(df['Process Name'].unique()[0]):
            logging.info("Process name error. The decoded Process name is: "+str(df['Process Name'].unique()[0]))
            return False
        if ILLEGAL_CHARACTERS_RE.search(df['Start Time'].unique()[0]):
            logging.info("Start Time error. The decoded Start Time is: "+str(df['Start Time'].unique()[0]))
            return False
        if ILLEGAL_CHARACTERS_RE.search(df['barcode'].unique()[0]):
            logging.info("Barcode error. The decoded Barcode is: "+str(df['barcode'].unique()[0]))
            return False
        if(len(df['barcode'].unique()[0])!=12):
            logging.info("Barcode length error. The decoded Barcode is: "+str(df['barcode'].unique()[0]))
            return False
   
    return True

def get_process_name(inpath):
    with open(inpath, 'rb') as f:
        data = f.read()
        t = -1
        x8=data.find(b'BTS Client')
        for i in range(x8-160,x8-100):
            if data[i:i+1] == b'\x00':
                t = i
                break
        process_name=data[x8-160:t]
    return process_name.decode()


def get_st_time(inpath):
    with open(inpath, 'rb') as f:
        data = f.read()
        pos=data.find(b'BTS Client')

        st_time2=data[pos-566:pos-547].decode()


        return(st_time2)
    
def get_barcode(inpath):
    with open(inpath, 'rb') as f:
        data = f.read()
        t = -1
        x8=data.find(b'BTS Client')
        for i in range(x8-260,x8):
            if data[i:i+1] == b'\x00':
                t = i
                break
        barcode=data[x8-260:t]
    return barcode.decode()

def get_remarks(inpath):
    with open(inpath, 'rb') as f:
        data = f.read()
        t = -1
        x8=data.find(b'BTS Client')
        for i in range(x8-376,x8):
            if data[i:i+1] == b'\x00':
                t = i
                break
        remarks=data[x8-376:t]
    return remarks.decode()

def ValidFile(inpath):
    with open(inpath, 'rb') as f:
        data = f.read()
        pos=data.find(b'BTS Client')
        if(pos==-1):
            return False
        else:
            return True

        

def _count_changes(series):
    """Enumerate the number of value changes in a series"""
    a = series.diff()
    a.iloc[0] = 1
    a.iloc[-1] = 0
    return((abs(a) > 0).cumsum())




def single_validator(List):

    
    
# #!IMPORTRANT TEMPORARY CODE FOR TESTING PUPOSES PLS REMOVE    
    # return True
# #!IMPORTRANT TEMPORARY CODE FOR TESTING PUPOSES PLS REMOVE
    
    if(List[0]<1 or List[1]<1 or List[2]<1 or List[4]<0 or List[5]<2 or List[7]<0 or List[8]<0): 

        return False #Index/Record_ID #cycle #step_ID #time_in_step #voltage #capacity #energy
    if ILLEGAL_CHARACTERS_RE.search(List[3]):
        return False

    return True



# Return a dict containing the relevant data.  all nice and pretty like.
def _bytes_to_list(byte_stream):

    List=[]

    state_dict = {
        1: 'CC_Chg',
        2: 'CC_Dchg',
        3: 'CV_Chg',
        4: 'Rest',
        5: 'Cycle',
        7: 'CCCV_Chg',
        10: 'CR_Dchg',
        13: 'Pause',
        16: 'Pulse',
        17: 'SIM',
        19: 'CV_Dchg',
        20: 'CCCV_Dchg'
    }
    range_list=[200000,-100000,-60000,-30000,-50000,-20000,-12000,-10000,-6000,-5000,-3000,-1000,-500,-100,0,10,100,200,1000,6000,12000,50000,60000]
    multiplier_dict = {
        -200000: 1e-2,
        -100000: 1e-2,
        -60000: 1e-2,
        -12000: 1e-2,
        -30000: 1e-2,
        -50000: 1e-2,
        -20000: 1e-2,
        -10000: 1e-2, 
        
        -6000: 1e-2,
        -5000: 1e-2,
        -3000: 1e-2,
        -1000: 1e-2,
        -500: 1e-3,
        -100: 1e-3,
        0: 0,
        10: 1e-3,
        100: 1e-2,
        200: 1e-2,
        1000: 1e-1,
        6000: 1e-1,
        12000: 1e-1,
        50000: 1e-1,
        60000: 1e-1,
    }


    [Index, Cycle] = struct.unpack('<IH', byte_stream[2:8])
    # [Step] = struct.unpack('<I', byte_stream[10:14])
    [Status, Jump, Time] = struct.unpack('<BBQ', byte_stream[12:22])    #state_dict[Status] is step_name
    [Step] = struct.unpack('<H', byte_stream[10:12])
    [Voltage, Current] = struct.unpack('<ii', byte_stream[22:30])
    [Charge_capacity, Discharge_capacity] = struct.unpack('<qq', byte_stream[38:54])
    [Charge_energy, Discharge_energy] = struct.unpack('<qq', byte_stream[54:70])
    [Y, M, D, h, m, s] = struct.unpack('<HBBBBB', byte_stream[70:77])
    [Range] = struct.unpack('<i', byte_stream[78:82])

    try:
        Date = datetime.datetime(Y, M, D, h, m, s)

    except ValueError:
        [Timestamp] = struct.unpack('<Q', byte_stream[70:78])
        Date = datetime.datetime.fromtimestamp(Timestamp)

    if(Range in range_list):
        multiplier = multiplier_dict[Range]
    else:
        raise ValueError("At ",Index," the ",Range," range caused an error")
    
    List = [
        Index,
        Cycle + 1,
        Step,
        state_dict[Status],
        Time/1000,
        Voltage/10000,
        Current*multiplier,
        (Charge_capacity+Discharge_capacity) *multiplier/3600,
        (Charge_energy+Discharge_energy)*multiplier/3600,
        Date,
    ]

   
    List.append(single_validator(List))
    return List
    
        
    
def _aux_bytes_to_list(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux] = struct.unpack('<B', bytes[1:2])
    [Index, Cycle] = struct.unpack('<IB', bytes[2:7])
    [T] = struct.unpack('<h', bytes[34:36])

    list = [
        Index,
        Aux,
        T/10
    ]

    return(list)






def _valid_record(bytes):
    """Helper function to identify a valid record"""
    # Check for a non-zero Status
    [Status] = struct.unpack('<B', bytes[12:13])
    return(Status != 0)


#! Output for newest BTSDA version
def nda_in_df_out(file):
  
    
    rec_columns = [
    'record_ID', 'cycle' ,'step_ID','step_name', 'time_in_step', 'voltage_V',
    'current_mA', 'capacity_mAh','energy_mWh','timestamp','Validated']
    aux_columns = ['Index', 'Aux', 'T']

    record_len = 86
    main_data = False
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        if mm.read(6) != b'NEWARE':
            raise ValueError(f"{file} does not appear to be a Neware file.")

        if(mm.find(b'BTS Client')==-1):
            raise ValueError(f"{file} does not appear to be a correctly downloaded Neware file.")
    
        identifier = b'\x00\x00\x00\x00\x55\x00'
        header = mm.find(identifier)
        if header == -1:
            raise EOFError(f"File {file} does not contain any valid records.")
        while (((mm[header + 4 + record_len] != 85) | (not _valid_record(mm[header+4:header+4+record_len]))) if header + 4 + record_len < mm_size
               else False):
            header = mm.find(identifier, header + 4)
        mm.seek(header + 4)

        output = []
        aux = []
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            if len(bytes) == record_len:

                # Check for a data record
                
                if (bytes[0:2] == b'\x55\x00'
                        and bytes[82:87] == b'\x00\x00\x00\x00'):
                                       
                    output.append( _bytes_to_list(bytes))

                # Check for an auxiliary record
                elif (bytes[0:1] == b'\x65'
                      and bytes[82:87] == b'\x00\x00\x00\x00'):
                    aux.append(_aux_bytes_to_list(bytes))

    df = pd.DataFrame(output, columns=rec_columns)
    
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    
    if(get_barcode(file).startswith('0AD')):
        df.drop(df.index[df['step_ID']<7],axis=0,inplace=True)
    df.record_ID = _count_changes(df.record_ID)
    df.step_ID = _count_changes(df.step_ID)

    if not df.record_ID.is_monotonic_increasing:
        df.sort_values('record_ID', inplace=True)

    df.reset_index(drop=True, inplace=True)
    
    #todo sets timegap records Validation to true
    validate_timegap(df)
    
    # Join temperature data
    aux_df = pd.DataFrame(aux, columns=aux_columns)


   

    aux_df.drop_duplicates(inplace=True)
    if not aux_df.empty:
        for Aux in aux_df.Aux.unique():
            df = df.join(aux_df.loc[aux_df.Aux == Aux, 'T']) # type: ignore
            df.rename(columns={'T': f"T{Aux}"}, inplace=True)
   
    
    #!DCIR Calculation
    df['prev_cur']=df['current_mA'].shift(periods=1)
    df['prev_vol']=df['voltage_V'].shift(periods=1)
    df['DCIR']=-1
    df.loc[((df['prev_cur']==0)&(df['current_mA']!=0)),'DCIR']=abs((df['voltage_V']-df['prev_vol'])/(df['current_mA']-df['prev_cur']))*1000
    df.drop(columns=['prev_cur','prev_vol'],inplace=True)

    dtype_dict = {
        'record_ID': 'uint32',
        'cycle': 'uint32',
        'step_ID': 'uint32',
        'step_name': 'str',
        'time_in_step': 'uint32',
        'voltage_V': 'float32',
        'current_mA': 'float32',
        'capacity_mAh': 'float32',
        'energy_mWh': 'float32',
        'DCIR': 'float32',
        'Validated':'bool'
    }
    df = df.astype(dtype=dtype_dict)
    return df
   



