def get_python_template(config):
    """Generate Python code template for data preparation"""

    def get_query_conditions(var_type):
        """Get the appropriate SQL conditions based on variable type"""
        if var_type == 'medication':
            return """
                    AND source_concept_id IN (
                        SELECT DISTINCT concept_id 
                        FROM {os.environ['WORKSPACE_CDR']}.concept 
                        WHERE vocabulary_id IN ('RxNorm', 'ATC')
                    )"""
        elif var_type == 'procedure':
            return """
                    AND source_concept_id IN (
                        SELECT DISTINCT concept_id 
                        FROM {os.environ['WORKSPACE_CDR']}.concept 
                        WHERE vocabulary_id IN ('CPT4', 'ICD10PCS', 'HCPCS')
                    )"""
        else:  # condition
            return """
                    AND source_concept_id IN (
                        SELECT DISTINCT concept_id 
                        FROM {os.environ['WORKSPACE_CDR']}.concept 
                        WHERE vocabulary_id IN ('ICD10CM', 'SNOMED')
                    )"""

    # Format the exposure and outcome variable lists
    exposure_vars = config['exposure_var']
    outcome_vars = config['outcome_var']
    exclusion_vars = config.get('exclusion_var', [])

    # Get the appropriate conditions for each variable type
    exposure_conditions = get_query_conditions(config['exposure_type'])
    outcome_conditions = get_query_conditions(config['outcome_type'])
    exclusion_conditions = get_query_conditions(config['exclusion_type']) if config['exclusion_type'] else ""

    code = """import os
import subprocess
import numpy as np
import pandas as pd

# SQL query to fetch EHR data
ehr_query = f\"\"\"    SELECT
        person.person_id,
        person.birth_datetime AS date_of_birth,
        p_race_concept.concept_name AS race,
        p_ethnicity_concept.concept_name AS ethnicity,
        p_sex_at_birth_concept.concept_name AS sex_at_birth
    FROM {os.environ['WORKSPACE_CDR']}.person person
    LEFT JOIN {os.environ['WORKSPACE_CDR']}.concept p_race_concept
        ON person.race_concept_id = p_race_concept.concept_id
    LEFT JOIN {os.environ['WORKSPACE_CDR']}.concept p_ethnicity_concept
        ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id
    LEFT JOIN {os.environ['WORKSPACE_CDR']}.concept p_sex_at_birth_concept
        ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id
    WHERE person.PERSON_ID IN (
        SELECT DISTINCT person_id
        FROM {os.environ['WORKSPACE_CDR']}.cb_search_person
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
variable_1 = {exposure_vars}  # {config['exposure_type'].title()}
variable_2 = {outcome_vars}   # {config['outcome_type'].title()}
variable_3 = {exclusion_vars}  # {config['exclusion_type'].title() if config['exclusion_type'] else 'No exclusion criteria'}

def create_cohort_query(concept_ids, var_type):
    \"\"\"
    Creates a SQL query for a cohort based on concept IDs and variable type
    \"\"\"
    concept_ids_str = ', '.join(map(str, concept_ids))

    query = f\"\"\"
    SELECT DISTINCT person_id 
    FROM {{os.environ['WORKSPACE_CDR']}}.cb_search_all_events 
    WHERE concept_id IN ({{concept_ids_str}})
    \"\"\"

    if var_type == 'medication':
        query += \"\"\"
        AND source_concept_id IN (
            SELECT DISTINCT concept_id 
            FROM {{os.environ['WORKSPACE_CDR']}}.concept 
            WHERE vocabulary_id IN ('RxNorm', 'ATC')
        )
        \"\"\"
    elif var_type == 'procedure':
        query += \"\"\"
        AND source_concept_id IN (
            SELECT DISTINCT concept_id 
            FROM {{os.environ['WORKSPACE_CDR']}}.concept 
            WHERE vocabulary_id IN ('CPT4', 'ICD10PCS', 'HCPCS')
        )
        \"\"\"
    else:  # condition
        query += \"\"\"
        AND source_concept_id IN (
            SELECT DISTINCT concept_id 
            FROM {{os.environ['WORKSPACE_CDR']}}.concept 
            WHERE vocabulary_id IN ('ICD10CM', 'SNOMED')
        )
        \"\"\"

    return query

# Initialize new columns in ehr_df for variables
ehr_df['var_1'] = 0
ehr_df['var_2'] = 0

# Fetch and update cohort for Variable 1 (Exposure)
if variable_1:
    query_var_1 = create_cohort_query(variable_1, "{config['exposure_type']}")
    cohort_var_1_df = pd.read_gbq(query_var_1, dialect="standard", use_bqstorage_api=True)
    ehr_df.loc[ehr_df['person_id'].isin(cohort_var_1_df['person_id']), 'var_1'] = 1

# Fetch and update cohort for Variable 2 (Outcome)
if variable_2:
    query_var_2 = create_cohort_query(variable_2, "{config['outcome_type']}")
    cohort_var_2_df = pd.read_gbq(query_var_2, dialect="standard", use_bqstorage_api=True)
    ehr_df.loc[ehr_df['person_id'].isin(cohort_var_2_df['person_id']), 'var_2'] = 1

# Apply exclusion criteria if specified
if variable_3:
    query_var_3 = create_cohort_query(variable_3, "{config['exclusion_type']}")
    cohort_var_3_df = pd.read_gbq(query_var_3, dialect="standard", use_bqstorage_api=True)
    exclusion_ids = set(cohort_var_3_df['person_id'])
    ehr_df = ehr_df[~ehr_df['person_id'].isin(exclusion_ids)]

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
print(f"Exposure Variable ({config['exposure_type'].title()}) SNOMED Codes: {{variable_1}}")
print(f"Outcome Variable ({config['outcome_type'].title()}) SNOMED Codes: {{variable_2}}")
if variable_3:
    print(f"Exclusion Criteria ({config['exclusion_type'].title()}) SNOMED Codes: {{variable_3}}")
else:
    print("No exclusion criteria specified")
"""

    return code