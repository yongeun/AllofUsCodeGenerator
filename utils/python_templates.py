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
        p_sex_at_birth_concept.concept_name AS sex_at_birth,
        p_insurance.concept_name AS insurance_status,
        p_education.concept_name AS education_status,
        p_income.concept_name AS income_status,
        p_smoking.concept_name AS smoking_status
    FROM {{os.environ['WORKSPACE_CDR']}}.person person
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_race_concept
        ON person.race_concept_id = p_race_concept.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_ethnicity_concept
        ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_sex_at_birth_concept
        ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_insurance
        ON person.insurance_concept_id = p_insurance.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_education
        ON person.education_concept_id = p_education.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_income
        ON person.income_concept_id = p_income.concept_id
    LEFT JOIN {{os.environ['WORKSPACE_CDR']}}.concept p_smoking
        ON person.smoking_status_concept_id = p_smoking.concept_id
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

def categorize_age_group(age_group_label):
    mapping = {{'18-34': 0, '35-49': 1, '50-64': 2, '65+': 3}}
    return mapping.get(age_group_label, 99)

def categorize_insurance(insurance):
    if pd.isna(insurance) or insurance in ['Unknown', 'None', 'Declined to Answer']:
        return 99
    return {{
        'Private': 1,
        'Medicare': 2,
        'Medicaid': 3,
        'Other': 4,
        'Uninsured': 5
    }}.get(insurance, 99)

def categorize_education(education):
    if pd.isna(education) or education in ['Unknown', 'Declined to Answer']:
        return 99
    return {{
        'Less than High School': 1,
        'High School/GED': 2,
        'Some College': 3,
        'College Graduate': 4,
        'Post-Graduate': 5
    }}.get(education, 99)

def categorize_income(income):
    if pd.isna(income) or income in ['Unknown', 'Declined to Answer']:
        return 99
    return {{
        'Less than $25,000': 1,
        '$25,000-$49,999': 2,
        '$50,000-$74,999': 3,
        '$75,000-$99,999': 4,
        '$100,000 or more': 5
    }}.get(income, 99)

def categorize_smoking(smoking):
    if pd.isna(smoking) or smoking in ['Unknown', 'Declined to Answer']:
        return 99
    return {{
        'Never Smoker': 1,
        'Former Smoker': 2,
        'Current Smoker': 3
    }}.get(smoking, 99)

# Apply categorization
ehr_df['age_group_code'] = ehr_df['age_group'].apply(categorize_age_group)
ehr_df['race_cat'] = ehr_df['race'].apply(categorize_race)
ehr_df['sex_cat'] = ehr_df['sex_at_birth'].apply(categorize_sex)
ehr_df['ethnicity_cat'] = ehr_df['ethnicity'].apply(categorize_ethnicity)
ehr_df['insurance_status'] = ehr_df['insurance_status'].apply(categorize_insurance)
ehr_df['education_status'] = ehr_df['education_status'].apply(categorize_education)
ehr_df['income_status'] = ehr_df['income_status'].apply(categorize_income)
ehr_df['active_smoking'] = ehr_df['smoking_status'].apply(categorize_smoking)

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
ehr_df = ehr_df[['person_id', 'age', 'age_group_code', 'race_cat', 'ethnicity_cat', 'sex_cat', 'var_1', 'var_2', 'insurance_status', 'education_status', 'income_status', 'active_smoking']]

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