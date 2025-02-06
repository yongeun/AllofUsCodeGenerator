import streamlit as st
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns

def preview_analysis(config):
    """Generate preview analysis based on sample data"""
    st.write("Preview Analysis (using sample data)")
    
    # Generate sample data
    np.random.seed(42)
    n_samples = 1000
    
    sample_data = {
        'age': np.random.normal(50, 15, n_samples),
        'sex_cat': np.random.choice([1, 2], n_samples),
        'race_cat': np.random.choice([1, 2, 3, 4, 5], n_samples),
        'ethnicity_cat': np.random.choice([1, 2], n_samples),
        'var_1': np.random.choice([0, 1], n_samples),
        'var_2': np.random.choice([0, 1], n_samples)
    }
    
    df = pd.DataFrame(sample_data)
    
    # Basic statistics
    st.subheader("Basic Statistics")
    st.write(df.describe())
    
    # Chi-square test
    contingency = pd.crosstab(df['var_1'], df['var_2'])
    chi2, p_value = stats.chi2_contingency(contingency)
    
    st.subheader("Chi-square Test Results")
    st.write(f"p-value: {p_value:.4f}")
    
    # Visualization
    st.subheader("Sample Visualizations")
    
    # Age distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.histplot(data=df, x='age', hue='var_2', multiple="stack")
    plt.title('Age Distribution by Outcome')
    st.pyplot(fig)
    
    # Outcome by exposure
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=df, x='var_1', y='var_2')
    plt.title('Outcome by Exposure')
    st.pyplot(fig)
