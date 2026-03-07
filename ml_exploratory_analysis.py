import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import joblib


load_dotenv()


client = bigquery.Client()

query = """
    SELECT * FROM `weather-data-pipeline-488717.weather_data.daily_weather_features`
    ORDER BY extraction_time
"""

df = client.query(query).to_dataframe()
df = df.ffill().bfill()

df.info()
print(df.isnull().sum())

y = df['high_wind_warning']
X = df[['Max_temperature', 'avg_wind_speed']]


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Training Features Shape: {X_train.shape}")
print(f"Testing Features Shape: {X_test.shape}")

model = RandomForestClassifier(class_weight='balanced', random_state=42)

model.fit(X_train, y_train)

predictions = model.predict(X_test)

accuracy = accuracy_score(y_test, predictions)
conf_matrix = confusion_matrix(y_test, predictions)

print(f"Random Forest Accuracy: {accuracy * 100}%")
print(f"Confusion Matrix:\n{conf_matrix}")

model_filename = 'random_forest_weather_model.joblib'
joblib.dump(model, model_filename)

print(f"\nModel successfully serialized and saved as: {model_filename}")
