def get_python_template(config):
    """Generate Python code template for data preparation"""

    code = """import os
import subprocess
import numpy as np
import pandas as pd
from google.cloud import storage

# SQL query to fetch EHR data
ehr_query = f\"\"\"    SELECT
        person.person_id,
        person.birth_datetime AS date_of_birth,
        p_race_concept.concept_name AS race,
        p_ethnicity_concept.concept_name AS ethnicity,
        p_sex_at_birth_concept.concept_name AS sex_at_birth
    FROM {{os.environ['WORKSPACE_CDR']}}.person person
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_race_concept
        ON person.race_concept_id = p_race_concept.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_ethnicity_concept
        ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_sex_at_birth_concept
        ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id
    WHERE person.PERSON_ID IN (
        SELECT DISTINCT person_id
        FROM {{os.environ['WORKSPACE_CDR']}}.cb_search_person
        WHERE has_ehr_data = 1
    )
\"\"\"

# Load data from BigQuery
ehr_df = pd.read_gbq(ehr_query, dialect="standard", use_bqstorage_api=True)

# Convert 'date_of_birth' to tz-naive datetime
ehr_df['date_of_birth'] = pd.to_datetime(ehr_df['date_of_birth']).dt.tz_localize(None)

# Calculate age
ehr_df['age'] = (pd.to_datetime('today') - ehr_df['date_of_birth']).dt.days // 365

# Define bins and labels for age grouping
age_bins = [18, 35, 50, 65, float('inf')]
age_labels = ['18-34', '35-49', '50-64', '65+']
ehr_df['age_group'] = pd.cut(ehr_df['age'], bins=age_bins, labels=age_labels, right=False)

# Function to categorize age group into numeric codes
def categorize_age_group(age_group_label):
    mapping = {{'18-34': 0, '35-49': 1, '50-64': 2, '65+': 3}}
    return mapping.get(age_group_label, 99)

# Categorization functions
def categorize_race(race):
    if pd.isna(race) or race in ['None Indicated', 'PMI: Skip', 'More than one population', 'None of these', 'I prefer not to answer']:
        return 99
    return {{
        'White': 1,
        'Black or African American': 2,
        'Asian': 3,
        'Middle Eastern or North African': 4,
        'Native Hawaiian or Other Pacific Islander': 5
    }}.get(race, 99)

def categorize_sex(sex):
    if pd.isna(sex) or sex in ['PMI: Skip', 'No matching concept', 'I prefer not to answer', 'None', 'Intersex']:
        return 99
    return {{'Female': 1, 'Male': 2}}.get(sex, 99)

def categorize_ethnicity(ethnicity):
    if pd.isna(ethnicity) or ethnicity in ['PMI: Skip', 'What Race Ethnicity: Race Ethnicity None Of These', 'PMI: Prefer Not To Answer', 'No matching concept']:
        return 99
    return {{'Not Hispanic or Latino': 1, 'Hispanic or Latino': 2}}.get(ethnicity, 99)

# Apply categorization
ehr_df['age_group_code'] = ehr_df['age_group'].apply(categorize_age_group)
ehr_df['race_cat'] = ehr_df['race'].apply(categorize_race)
ehr_df['sex_cat'] = ehr_df['sex_at_birth'].apply(categorize_sex)
ehr_df['ethnicity_cat'] = ehr_df['ethnicity'].apply(categorize_ethnicity)

# Add variables for association study
variable_1 = {exposure_vars}  # Exposure variable
variable_2 = {outcome_vars}   # Outcome variable
variable_3 = []  # Exclusion criteria (if any)

def create_cohort_query(concept_ids):
    concept_ids_str = ', '.join(map(str, concept_ids))
    query = f\"\"\"
    SELECT person.person_id 
    FROM {{os.environ['WORKSPACE_CDR']}}.person person   
    WHERE person.PERSON_ID IN (
        SELECT DISTINCT person_id  
        FROM {{os.environ['WORKSPACE_CDR']}}.cb_search_person cb_search_person  
        WHERE cb_search_person.person_id IN (
            SELECT criteria.person_id 
            FROM (
                SELECT DISTINCT person_id, entry_date, concept_id 
                FROM {{os.environ['WORKSPACE_CDR']}}.cb_search_all_events 
                WHERE concept_id IN (
                    SELECT DISTINCT c.concept_id 
                    FROM {{os.environ['WORKSPACE_CDR']}}.cb_criteria c 
                    JOIN (
                        SELECT CAST(cr.id as string) AS id       
                        FROM {{os.environ['WORKSPACE_CDR']}}.cb_criteria cr       
                        WHERE concept_id IN ({{concept_ids_str}})       
                        AND full_text LIKE '%_rank1]%'
                    ) a ON (
                        c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id
                    ) 
                    WHERE is_standard = 0 AND is_selectable = 1
                ) AND is_standard = 0
            )
        ) criteria
    )
    \"\"\"
    return query

# Initialize columns for variable_1 and variable_2
ehr_df['var_1'] = 0
ehr_df['var_2'] = 0

# Update cohort for Variable 1 (Exposure)
query_var_1 = create_cohort_query(variable_1)
cohort_var_1_df = pd.read_gbq(query_var_1, dialect="standard",
                             use_bqstorage_api=True)
ehr_df.loc[ehr_df['person_id'].isin(cohort_var_1_df['person_id']), 'var_1'] = 1

# Update cohort for Variable 2 (Outcome)
query_var_2 = create_cohort_query(variable_2)
cohort_var_2_df = pd.read_gbq(query_var_2, dialect="standard",
                             use_bqstorage_api=True)
ehr_df.loc[ehr_df['person_id'].isin(cohort_var_2_df['person_id']), 'var_2'] = 1

# Apply exclusion criteria if specified
if variable_3:
    query_var_3 = create_cohort_query(variable_3)
    cohort_var_3_df = pd.read_gbq(query_var_3, dialect="standard",
                                 use_bqstorage_api=True)
    ehr_df = ehr_df[~ehr_df['person_id'].isin(cohort_var_3_df['person_id'])]

# Keep only necessary columns
ehr_df = ehr_df[['person_id', 'age', 'age_group_code', 'race_cat', 'ethnicity_cat', 'sex_cat', 'var_1', 'var_2']]

# Save to Google Bucket
destination_filename = 'ehr_df.csv'
ehr_df.to_csv(destination_filename, index=False)

# Copy to Google Bucket
my_bucket = os.getenv('WORKSPACE_BUCKET')
args = ["gsutil", "cp", f"./{{destination_filename}}", f"{{my_bucket}}/data/"]
output = subprocess.run(args, capture_output=True)

print("Data processing complete and saved to Google Bucket")
print(f"Exposure Variable SNOMED Codes: {{variable_1}}")
print(f"Outcome Variable SNOMED Codes: {{variable_2}}")
print(f"Exclusion Criteria SNOMED Codes: {{variable_3}}")
""".format(
        exposure_vars=config['exposure_var'],
        outcome_vars=config['outcome_var']
    )

    return code