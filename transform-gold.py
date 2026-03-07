import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
load_dotenv()

def gold_layer_main_function():
    
    gcp_project = os.getenv('GCP_PROJECT_ID')    

    data_path=os.path.join(os.getenv('SILVER_STORAGE_PATH'),'weather_data.parquet')
    print(f"Reading Silver data from {data_path}...")
    gold_df=pd.read_parquet(data_path)    

    print("Aggregating daily features...")
    gold_dir_path=os.getenv('GOLD_LAYER_PATH')
    os.makedirs(gold_dir_path,exist_ok=True)
    agg_gold_df=gold_df.groupby(pd.Grouper(key='extraction_time',freq='D')).agg(Max_temperature=('temperature','max'),avg_wind_speed=('wind_speed','mean')).reset_index()
    agg_gold_df['high_wind_warning']=agg_gold_df['avg_wind_speed']>6    

    destination = 'weather_data.daily_weather_features'
    print(f"Pushing Gold features to BigQuery: {gcp_project}.{destination}...")    
    

    client = bigquery.Client(project=gcp_project)
    dataset_ref = f"{gcp_project}.weather_data"    

    print(f"Checking infrastructure for dataset: {dataset_ref}...")
    try:
        client.get_dataset(dataset_ref)
        print("✅ Dataset exists. Proceeding to load.")
    except NotFound:
        print("⚠️ Dataset not found. Provisioning new dataset...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        dataset = client.create_dataset(dataset, timeout=30)
        print("✅ Dataset successfully created.")    
    
    

    try:
        agg_gold_df.to_gbq(
            destination_table=destination,
            project_id=gcp_project,
            if_exists='replace'
        )
        print("✅ Load to BigQuery complete.")
    except Exception as e:
        print(f"❌ CRITICAL FAILURE during BigQuery load:\n{e}")
        
if __name__=="__main__":
    gold_layer_main_function()
    
