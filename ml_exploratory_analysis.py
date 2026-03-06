import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import joblib

# Load your Google Cloud credentials
load_dotenv()

# Initialize the BigQuery Client
client = bigquery.Client()

# Write the chronological query
query = """
    SELECT * FROM `weather-data-pipeline-488717.weather_data.daily_weather_features`
    ORDER BY extraction_time
"""

# Execute the query and pull into local memory
df = client.query(query).to_dataframe()
df = df.ffill().bfill()
# Show the architectural shape of the data
df.info()
print(df.isnull().sum())

y = df['high_wind_warning']
X = df[['Max_temperature', 'avg_wind_speed']]

# 3. Split the data (80% Training, 20% Testing)
# random_state=42 ensures we both get the exact same random split for debugging
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 4. Prove the architecture
print(f"Training Features Shape: {X_train.shape}")
print(f"Testing Features Shape: {X_test.shape}")

# 1. Initialize the Advanced Algorithm
model = RandomForestClassifier(class_weight='balanced', random_state=42)

# 2. Train the Model (Build the 100 trees)
model.fit(X_train, y_train)

# 3. The Exam (Force the trees to vote on the 6 hidden days)
predictions = model.predict(X_test)

# 4. Grade the Exam
accuracy = accuracy_score(y_test, predictions)
conf_matrix = confusion_matrix(y_test, predictions)

print(f"Random Forest Accuracy: {accuracy * 100}%")
print(f"Confusion Matrix:\n{conf_matrix}")

model_filename = 'random_forest_weather_model.joblib'
joblib.dump(model, model_filename)

print(f"\nModel successfully serialized and saved as: {model_filename}")
