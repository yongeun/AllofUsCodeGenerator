def get_python_template(config):
    """Generate Python code template"""

    code = f"""import os
import pandas as pd
import numpy as np
from google.cloud import storage
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# Configuration
EXPOSURE_VARS = {config['exposure_var']}
OUTCOME_VARS = {config['outcome_var']}

def load_data():
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Get bucket name from environment
    bucket_name = os.getenv('WORKSPACE_BUCKET')
    bucket = storage_client.get_bucket(bucket_name)

    # Load data from CSV in bucket
    blob = bucket.blob('data/ehr_df.csv')
    return pd.read_csv(blob.download_as_string())

def preprocess_data(df):
    # Data type conversions
    categorical_cols = ['race_cat', 'ethnicity_cat', 'sex_cat']
    df[categorical_cols] = df[categorical_cols].astype('category')

    return df

def perform_analysis(df):
    # Basic statistics
    print("\\nBasic Statistics:")
    print(df.describe())

    # Chi-square test for categorical variables
    contingency = pd.crosstab(df['var_1'], df['var_2'])
    chi2, p_value = stats.chi2_contingency(contingency)
    print(f"\\nChi-square test p-value: {p_value:.4f}")

    # Logistic regression
    from statsmodels.formula.api import logit
    model = logit('var_2 ~ var_1 + age + C(sex_cat) + C(race_cat) + C(ethnicity_cat)', data=df)
    results = model.fit()
    print("\\nLogistic Regression Results:")
    print(results.summary())
"""

    if config['include_visualization']:
        code += """
def create_visualizations(df):
    # Create visualization directory if it doesn't exist
    os.makedirs('visualizations', exist_ok=True)

    # Age distribution plot
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='age', hue='var_2', multiple="stack")
    plt.title('Age Distribution by Outcome')
    plt.savefig('visualizations/age_distribution.png')
    plt.close()

    # Outcome by exposure
    plt.figure(figsize=(8, 6))
    sns.barplot(data=df, x='var_1', y='var_2')
    plt.title('Outcome by Exposure')
    plt.savefig('visualizations/outcome_by_exposure.png')
    plt.close()
"""

    code += """
def main():
    # Load and preprocess data
    df = load_data()
    df = preprocess_data(df)

    # Perform analysis
    perform_analysis(df)
    """

    if config['include_visualization']:
        code += """
    # Create visualizations
    create_visualizations(df)
"""

    code += """
if __name__ == "__main__":
    main()
"""

    return code