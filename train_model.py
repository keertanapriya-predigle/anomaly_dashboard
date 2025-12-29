# -*- coding: utf-8 -*-
"""
Created on Wed Dec 24 14:17:06 2025

@author: keertanapriya
"""
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

df = pd.read_csv('ecommerce_dataset_updated.csv')
print("Columns:", df.columns.tolist())
print(df.head())
features = df[['Price (Rs.)', 'Final_Price(Rs.)']]
print("Features shape:", features.shape)

model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
model.fit(features)
joblib.dump(model, "anomaly_model.pkl")
print("Model trained and saved!")
