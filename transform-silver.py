import pandas as pd
import glob, json
import os
from datetime import datetime
from dotenv import load_dotenv
import shutil

load_dotenv()
def safe_get(data, keys, default=None):
    """
    Safely navigate a nested dictionary.
    Usage: safe_get(result, ['raw_payload', 'current_weather', 'temperature'])
    """
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return default
    return data

def batch_health_summary(nrows,duplicates_num,null_num,output_path):
    os.makedirs(output_path,exist_ok=True)
    with open(os.path.join(output_path,'Batch_health_log_{}.txt'.format(datetime.now().date())),'a') as f:
        f.write(f'Batch number of rows processed:{nrows} \n')
        f.write(f'Batch number of duplicates found and erased:{duplicates_num} \n')
        f.write(f'Batch number of null values found at temperature column:{null_num} \n')

def warning_log(output_path,warning_list:list):
        with open(output_path,'a') as f:
            for warn in warning_list:
                file_name = warn[0]
                warning_type = warn[1]
                if warning_type=='warning':
                    f.write(f'Time:{datetime.now().time()}. Missing data at file {os.path.basename(file_name)} \n')
                elif warning_type=='critical':
                    f.write(f'Time:{datetime.now().time()}. Missing data from critical column (extraction_time) at file {os.path.basename(file_name)} \n')
                else:
                    f.write(f'Time:{datetime.now().time()}. Fatal error: critical column (extraction_time) at file {os.path.basename(file_name)} NOT FOUND \n')


def dataframe_creation(data_list:list,health_log_file,output_path):
    silver_df=pd.DataFrame(data_list)
    silver_df['extraction_time']=pd.to_datetime(silver_df['extraction_time'])
    silver_df['processed_at']=pd.to_datetime(silver_df['processed_at'])
    silver_df['temperature']=pd.to_numeric(silver_df['temperature'],errors='coerce')
    num_of_duplicates=silver_df[silver_df.duplicated(subset=['extraction_time'])].shape[0]
    num_of_nulls=silver_df['temperature'].isnull().sum()
    silver_df_nrows=silver_df.shape[0]
    batch_health_summary(silver_df_nrows,num_of_duplicates,num_of_nulls,health_log_file)
    silver_df.drop_duplicates(subset=['extraction_time'],keep='last',inplace=True)
    os.makedirs(output_path,exist_ok=True)
    batch_filename = f"weather_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    silver_df.to_parquet(os.path.join(output_path,batch_filename),index=False)
                    
                    
def silver_layer_main_function():
    
    warning_log_file_path=os.getenv('WARNINGS_LOG_PATH')
    health_log_path=os.getenv('HEALTH_LOG_PATH')
    bronze_path=os.getenv('RAW_STORAGE_PATH')
    output_path=os.getenv('SILVER_STORAGE_PATH')
    log_filename = f"warnings_log_{datetime.now().date()}.txt"
    quarantine_path=os.getenv('QUARANTINE_PATH')
    full_log_path = os.path.join(warning_log_file_path, log_filename)
    archive_path=os.getenv('ARCHIVE_PATH')
    os.makedirs(quarantine_path,exist_ok=True)
    os.makedirs(archive_path,exist_ok=True)


    
    data_list=[]
    warning_list=[]
    
    for f in glob.glob(f"{os.path.abspath(bronze_path)}/*.json"):
        weather_dict={}
        corrupt_file=False
        with open(f, "rb") as infile:
            try:
                result=(json.load(infile))
                weather_dict['extraction_time']=safe_get(result,['metadata','extracted_at (Time of the extracting location)'])
                weather_dict['processed_at']=datetime.now()
                weather_dict['temperature']=safe_get(result,['raw_payload','current_weather','temperature'])
                weather_dict['wind_speed']=safe_get(result,['raw_payload','current_weather','windspeed'])
                weather_dict['source_id']='open_weather_v1'
            except Exception as e:
                print('Critical error at file {} -> continuing to the next file'.format(os.path.basename(f)))
                warning_list.append([f,'fatal'])
                corrupt_file=True
        if corrupt_file:
            os.rename(f,os.path.join(quarantine_path,os.path.basename(f)))
            continue
        for col in ['temperature','wind_speed']:
            if weather_dict[col]==None:
                print(f'Missing data at the file {os.path.basename(f)}')
                warning_list.append([f,'warning'])
        if weather_dict['extraction_time']==None:
            print(f'Missing data from critical column (extraction_time) at file{os.path.basename(f)}')
            warning_list.append([f,'critical'])
            os.rename(f,os.path.join(quarantine_path,os.path.basename(f)))           
            continue
        data_list.append(weather_dict)
        os.rename(f,os.path.join(archive_path,os.path.basename(f)))    


    if warning_list:
        os.makedirs(warning_log_file_path,exist_ok=True)
        warning_log(full_log_path,warning_list)
    if len(data_list)>0:
        dataframe_creation(data_list,health_log_path,output_path)
    else:
        print('No valid data found for today')
    

if __name__=="__main__":
    silver_layer_main_function()
    

