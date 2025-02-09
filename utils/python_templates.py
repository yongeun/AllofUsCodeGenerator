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

    def get_cohort_query(variable_config, variable_name):
        """Generate cohort query based on variable type"""
        if variable_config["type"] == "condition":
            icd9_codes = ", ".join(f"'{code}'" for code in variable_config["icd9"])
            icd10_codes = ", ".join(f"'{code}'" for code in variable_config["icd10"])
            
            return f"""
# Get {variable_name} condition concepts
{variable_name}_sql = f\"\"\"
SELECT 
    c.concept_name,
    c.concept_code,
    c.concept_id
FROM 
    `{{os.environ['WORKSPACE_CDR']}}.concept` c
    JOIN `{{os.environ['WORKSPACE_CDR']}}.condition_occurrence` co
        ON c.concept_id = co.condition_source_concept_id
WHERE
    (vocabulary_id='ICD9CM' AND concept_code IN ({icd9_codes}))
    OR (vocabulary_id='ICD10CM' AND concept_code IN ({icd10_codes}))
GROUP BY
    c.concept_name,
    c.concept_code,
    c.concept_id
\"\"\"

{variable_name}_concepts_df = pd.read_gbq({variable_name}_sql, dialect="standard", use_bqstorage_api=True)
{variable_name}_concepts_string = ", ".join(map(str, {variable_name}_concepts_df['concept_id']))

# Create {variable_name} cohort query
{variable_name}_cohort_sql = f\"\"\"
SELECT DISTINCT person_id
FROM `{{os.environ['WORKSPACE_CDR']}}.condition_occurrence`
WHERE condition_source_concept_id IN ({{{variable_name}_concepts_string}})
\"\"\"
"""
        else:  # medication
            drug_names = variable_config["names"]
            drug_names_subquery = " OR ".join([f"LOWER(c.concept_name) LIKE '%{drug}%'" for drug in drug_names])
            
            return f"""
# Get {variable_name} medication concepts
{variable_name}_sql = f\"\"\"
SELECT
    DISTINCT c2.concept_name,
    c2.concept_code,
    c2.concept_id
FROM
    `{{os.environ['WORKSPACE_CDR']}}.concept` c
    JOIN `{{os.environ['WORKSPACE_CDR']}}.concept_ancestor` ca
        ON c.concept_id = ca.ancestor_concept_id
    JOIN `{{os.environ['WORKSPACE_CDR']}}.concept` c2
        ON c2.concept_id = ca.descendant_concept_id
WHERE
    c.concept_class_id = 'Ingredient'
    AND ({drug_names_subquery})
\"\"\"

{variable_name}_concepts_df = pd.read_gbq({variable_name}_sql, dialect="standard", use_bqstorage_api=True)
{variable_name}_concepts_string = ", ".join(map(str, {variable_name}_concepts_df['concept_id']))

# Create {variable_name} cohort query
{variable_name}_cohort_sql = f\"\"\"
SELECT DISTINCT person_id
FROM `{{os.environ['WORKSPACE_CDR']}}.drug_exposure`
WHERE drug_concept_id IN ({{{variable_name}_concepts_string}})
\"\"\"
"""

    # Format the exposure and outcome variable lists
    exposure_vars = config['exposure_var']
    outcome_vars = config['outcome_var']
    exclusion_vars = config.get('exclusion_var', [])

    code = """import os
import pandas as pd
import subprocess
import numpy as np

# SQL query to fetch EHR data
ehr_query = f\"\"\"
WITH ehr AS (
    SELECT
        DISTINCT p.person_id AS PERSON_ID,
        p.birth_datetime AS DATE_OF_BIRTH,
        c_race.concept_name AS RACE,
        c_sex.concept_name AS SEX,
        c_ethn.concept_name AS ETHNICITY
    FROM
        `{os.environ['WORKSPACE_CDR']}.person` p
        LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` c_race
            ON p.race_concept_id = c_race.concept_id
        LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` c_sex
            ON p.sex_at_birth_concept_id = c_sex.concept_id
        LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` c_ethn
            ON p.ethnicity_concept_id = c_ethn.concept_id
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.measurement` as m on p.person_id = m.person_id
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.measurement_ext` as mm on m.measurement_id = mm.measurement_id
    WHERE lower(mm.src_id) like 'ehr site%'

    union distinct

    SELECT
        DISTINCT p.person_id AS PERSON_ID,
        p.birth_datetime AS DATE_OF_BIRTH,
        c_race.concept_name AS RACE,
        c_sex.concept_name AS SEX,
        c_ethn.concept_name AS ETHNICITY
    FROM
        `{os.environ['WORKSPACE_CDR']}.person` p
        LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` c_race
            ON p.race_concept_id = c_race.concept_id
        LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` c_sex
            ON p.sex_at_birth_concept_id = c_sex.concept_id
        LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` c_ethn
            ON p.ethnicity_concept_id = c_ethn.concept_id
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.condition_occurrence` as m on p.person_id = m.person_id
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.condition_occurrence_ext` as mm on m.condition_occurrence_id = mm.condition_occurrence_id
    WHERE lower(mm.src_id) like 'ehr site%'
)

SELECT
    ehr.PERSON_ID,
    ehr.DATE_OF_BIRTH,
    ehr.RACE,
    ehr.ETHNICITY,
    ehr.SEX,
    ins1.aname AS insurance_status,
    obs1.aname AS education_level,
    obs2.aname AS income_level,
    obs3.aname AS smoking_status
FROM ehr

LEFT JOIN (
    SELECT 
        o.person_id, 
        answer.concept_name as aname
    FROM `{os.environ['WORKSPACE_CDR']}.observation` o
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` answer on (answer.concept_id=o.value_source_concept_id)
    WHERE o.observation_source_concept_id = 1585386
) ins1 ON ehr.PERSON_ID = ins1.person_id

LEFT JOIN (
    SELECT 
        o.person_id, 
        answer.concept_name as aname
    FROM `{os.environ['WORKSPACE_CDR']}.observation` o
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` answer on (answer.concept_id=o.value_source_concept_id)
    WHERE o.observation_source_concept_id = 1585375
) obs1 ON ehr.PERSON_ID = obs1.person_id

LEFT JOIN (
    SELECT 
        o.person_id, 
        answer.concept_name as aname
    FROM `{os.environ['WORKSPACE_CDR']}.observation` o
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` answer on (answer.concept_id=o.value_source_concept_id)
    WHERE o.observation_source_concept_id = 1585940
) obs2 ON ehr.PERSON_ID = obs2.person_id

LEFT JOIN (
    SELECT 
        o.person_id, 
        answer.concept_name as aname
    FROM `{os.environ['WORKSPACE_CDR']}.observation` o
    LEFT JOIN `{os.environ['WORKSPACE_CDR']}.concept` answer on (answer.concept_id=o.value_source_concept_id)
    WHERE o.observation_source_concept_id = 1586198
) obs3 ON ehr.PERSON_ID = obs3.person_id
\"\"\"

# Load data from BigQuery
ehr_df = pd.read_gbq(ehr_query, dialect="standard", use_bqstorage_api=True)

# Calculate age and create basic demographics
ehr_df['DATE_OF_BIRTH'] = pd.to_datetime(ehr_df['DATE_OF_BIRTH']).dt.tz_localize(None)
ehr_df['age'] = (pd.to_datetime('today') - ehr_df['DATE_OF_BIRTH']).dt.days // 365
ehr_df['age_group'] = pd.cut(ehr_df['age'], 
                            bins=[18, 40, 65, float('inf')], 
                            labels=['18-39', '40-64', '65+'], 
                            right=False)
ehr_df['age_group_code'] = pd.Categorical(ehr_df['age_group']).codes

# Create race/ethnicity categories
ehr_df['RACEETHNICITY'] = np.where(ehr_df['ETHNICITY'] == 'Hispanic or Latino', 'Hispanic',
    np.where(ehr_df['RACE'] == 'White', 'Non-Hispanic White',
    np.where(ehr_df['RACE'] == 'Black or African American', 'Non-Hispanic Black',
    np.where(ehr_df['RACE'] == 'Asian', 'Non-Hispanic Asian',
    np.where(ehr_df['RACE'] == 'Middle Eastern or North African', 'Non-Hispanic Middle Eastern or North African',
    np.where(ehr_df['RACE'] == 'Native Hawaiian or Other Pacific Islander', 'Non-Hispanic Native Hawaiian or Other Pacific Islander',
    'Other Race or Ethnicity'))))))

# Categorize demographics
ehr_df['sex_cat'] = np.where(ehr_df['SEX'] == 'Male', 0,
    np.where(ehr_df['SEX'] == 'Female', 1,
    np.where(ehr_df['SEX'].isna(), 998, 999)))

ehr_df['race_cat'] = np.where(ehr_df['RACE'] == 'White', 0,
    np.where(ehr_df['RACE'] == 'Black or African American', 1,
    np.where(ehr_df['RACE'] == 'Asian', 2,
    np.where(ehr_df['RACE'] == 'Middle Eastern or North African', 3,
    np.where(ehr_df['RACE'] == 'Native Hawaiian or Other Pacific Islander', 4,
    np.where(ehr_df['RACE'].isna(), 998, 999))))))

ehr_df['ethnicity_cat'] = np.where(ehr_df['ETHNICITY'] == 'Not Hispanic or Latino', 0,
    np.where(ehr_df['ETHNICITY'] == 'Hispanic or Latino', 1,
    np.where(ehr_df['ETHNICITY'].isna(), 998, 999)))

# Create combined race/ethnicity category
ehr_df['raceethnicity_cat'] = np.where(ehr_df['ethnicity_cat'] == 1, 2,  # Hispanic
    np.where((ehr_df['ethnicity_cat'] == 0) & (ehr_df['race_cat'] == 0), 0,  # Non-Hispanic White
    np.where((ehr_df['ethnicity_cat'] == 0) & (ehr_df['race_cat'] == 1), 1,  # Non-Hispanic Black
    np.where((ehr_df['ethnicity_cat'] == 0) & (ehr_df['race_cat'].isin([2, 3, 4])), 3,  # Non-Hispanic Asian/MENA/NHOPI
    999))))

# Categorize insurance status
ehr_df['insurance'] = np.where(ehr_df['ins1'] == 'Health Insurance: No', 0,
    np.where(ehr_df['ins1'] == 'Health Insurance: Yes', 1,
    np.where(ehr_df['ins1'].isna(), 998, 999)))

ehr_df['insurance_2'] = np.where(ehr_df['ins2'] == 'Insurance Type Update: None', 0,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: Medicare', 1,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: Medicaid', 2,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: Purchased', 3,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: Employer Or Union', 4,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: Military', 5,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: Other Health Plan', 6,
    np.where(ehr_df['ins2'] == 'Insurance Type Update: VA', 7,
    np.where(ehr_df['ins2'].isna(), 998, 999)))))))))

# Categorize income
ehr_df['income'] = np.where(ehr_df['aname1'] == 'Annual Income: less 10k', 0,
    np.where(ehr_df['aname1'] == 'Annual Income: 10k 25k', 1,
    np.where(ehr_df['aname1'] == 'Annual Income: 25k 35k', 2,
    np.where(ehr_df['aname1'] == 'Annual Income: 35k 50k', 3,
    np.where(ehr_df['aname1'] == 'Annual Income: 50k 75k', 4,
    np.where(ehr_df['aname1'] == 'Annual Income: 75k 100k', 5,
    np.where(ehr_df['aname1'] == 'Annual Income: 100k 150k', 6,
    np.where(ehr_df['aname1'] == 'Annual Income: 150k 200k', 7,
    np.where(ehr_df['aname1'] == 'Annual Income: more 200k', 8,
    np.where(ehr_df['aname1'].isna(), 998, 999))))))))))

# Categorize education
ehr_df['education'] = np.where(ehr_df['aname2'].isin(['Highest Grade: Never Attended', 'Highest Grade: One Through Four', 
    'Highest Grade: Five Through Eight', 'Highest Grade: Nine Through Eleven']), 0,
    np.where(ehr_df['aname2'] == 'Highest Grade: Twelve Or GED', 1,
    np.where(ehr_df['aname2'] == 'Highest Grade: College One to Three', 2,
    np.where(ehr_df['aname2'] == 'Highest Grade: College Graduate', 3,
    np.where(ehr_df['aname2'] == 'Highest Grade: Advanced Degree', 4,
    np.where(ehr_df['aname2'].isna(), 998, 999))))))

# Categorize smoking status
ehr_df['cigs'] = np.where(ehr_df['aname3'] == '100 Cigs Lifetime: No', 0,
    np.where(ehr_df['aname3'] == '100 Cigs Lifetime: Yes', 1,
    np.where(ehr_df['aname3'].isna(), 998, 999)))

ehr_df['cigs_frequency'] = np.where(ehr_df['aname4'].isin(['Smoke Frequency: Some Days', 'Smoke Frequency: Every Day']), 1,
    np.where(ehr_df['aname4'] == 'Smoke Frequency: Not At All', 0,
    np.where(ehr_df['aname4'].isna(), 998, 999)))

ehr_df['smoking'] = np.where(ehr_df['cigs_frequency'] == 1, 1,
    np.where(ehr_df['cigs'] == 0, 0,
    np.where((ehr_df['cigs'] == 1) & (ehr_df['cigs_frequency'] == 0), 0, 999)))

# Categorize alcohol use
ehr_df['alcohol'] = np.where(ehr_df['aname5'] == 'Alcohol Participant: No', 0,
    np.where(ehr_df['aname5'] == 'Alcohol Participant: Yes', 1,
    np.where(ehr_df['aname5'].isna(), 998, 999)))

ehr_df['alcohol_freq'] = np.where(ehr_df['aname6'] == 'Drink Frequency Past Year: Never', 0,
    np.where(ehr_df['aname6'] == 'Drink Frequency Past Year: Monthly Or Less', 1,
    np.where(ehr_df['aname6'] == 'Drink Frequency Past Year: 2 to 4 Per Month', 2,
    np.where(ehr_df['aname6'] == 'Drink Frequency Past Year: 2 to 3 Per Week', 3,
    np.where(ehr_df['aname6'] == 'Drink Frequency Past Year: 4 or More Per Week', 4,
    np.where(ehr_df['aname6'].isna(), 998, 999))))))

# Calculate alcohol consumption
ehr_df['avg_daily_drink_value'] = np.where(ehr_df['avg_daily_drink'] == 0, 1.5,
    np.where(ehr_df['avg_daily_drink'] == 1, 3.5,
    np.where(ehr_df['avg_daily_drink'] == 2, 5.5,
    np.where(ehr_df['avg_daily_drink'] == 3, 8,
    np.where(ehr_df['avg_daily_drink'] == 4, 11, 0)))))

ehr_df['alcohol_freq_value'] = np.where(ehr_df['alcohol_freq'] == 0, 0,
    np.where(ehr_df['alcohol_freq'] == 1, 0.25,
    np.where(ehr_df['alcohol_freq'] == 2, 0.75,
    np.where(ehr_df['alcohol_freq'] == 3, 2.5,
    np.where(ehr_df['alcohol_freq'] == 4, 4, 0)))))

ehr_df['weekly_alcohol_grams'] = ehr_df['alcohol_freq_value'] * ehr_df['avg_daily_drink_value'] * 14

# Determine alcohol exclusion
ehr_df['alcohol_exclusion'] = np.where(
    ((ehr_df['sex_cat'] == 1) & (ehr_df['weekly_alcohol_grams'] > 140)) |
    ((ehr_df['sex_cat'] == 0) & (ehr_df['weekly_alcohol_grams'] > 210)) |
    (ehr_df['heavy_drink_freq'] == 4), 1, 0)

# Initialize new columns for variables
ehr_df['var_1'] = 0  # Exposure variable
ehr_df['var_2'] = 0  # Outcome variable

# Add variables for association study
variable_1 = {exposure_vars}  # {config['exposure_type'].title()}
variable_2 = {outcome_vars}   # {config['outcome_type'].title()}
variable_3 = {exclusion_vars}  # {config['exclusion_type'].title() if config['exclusion_type'] else 'No exclusion criteria'}

def create_cohort_query(concept_ids, var_type):
    '''Creates a SQL query for a cohort based on concept IDs using hierarchical relationships'''
    concept_ids_str = ', '.join(map(str, concept_ids))
    
    query = f\"\"\"
    SELECT DISTINCT person_id 
    FROM `{{os.environ['WORKSPACE_CDR']}}.cb_search_all_events`
    WHERE concept_id IN ({{concept_ids_str}})
    \"\"\"
    return query

# Fetch and update cohort for Variable 1 (Exposure)
if variable_1:
    query_var_1 = create_cohort_query(variable_1, "{config['exposure_type']}")
    cohort_var_1_df = pd.read_gbq(query_var_1, dialect="standard", use_bqstorage_api=True)
    ehr_df.loc[ehr_df['PERSON_ID'].isin(cohort_var_1_df['person_id']), 'var_1'] = 1

# Fetch and update cohort for Variable 2 (Outcome)
if variable_2:
    query_var_2 = create_cohort_query(variable_2, "{config['outcome_type']}")
    cohort_var_2_df = pd.read_gbq(query_var_2, dialect="standard", use_bqstorage_api=True)
    ehr_df.loc[ehr_df['PERSON_ID'].isin(cohort_var_2_df['person_id']), 'var_2'] = 1

# Apply exclusion criteria if specified
if variable_3:
    query_var_3 = create_cohort_query(variable_3, "{config['exclusion_type']}")
    cohort_var_3_df = pd.read_gbq(query_var_3, dialect="standard", use_bqstorage_api=True)
    ehr_df = ehr_df[~ehr_df['PERSON_ID'].isin(cohort_var_3_df['person_id'])]

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

# Add exposure cohort creation
code += get_cohort_query(config["exposure"], "exposure")
code += """
exposure_cohort_df = pd.read_gbq(exposure_cohort_sql, dialect="standard", use_bqstorage_api=True)
"""

# Add outcome cohort creation
code += get_cohort_query(config["outcome"], "outcome")
code += """
outcome_cohort_df = pd.read_gbq(outcome_cohort_sql, dialect="standard", use_bqstorage_api=True)

# Add cohort flags to main dataframe
ehr_df['exposure'] = np.where(ehr_df['PERSON_ID'].isin(exposure_cohort_df['person_id']), 1, 0)
ehr_df['outcome'] = np.where(ehr_df['PERSON_ID'].isin(outcome_cohort_df['person_id']), 1, 0)

# Save to Google Bucket
destination_filename = 'ehr_df.csv'
ehr_df.to_csv(destination_filename, index=False)

my_bucket = os.getenv('WORKSPACE_BUCKET')
args = ["gsutil", "cp", f"./{{destination_filename}}", f"{{my_bucket}}/data/"]
output = subprocess.run(args, capture_output=True)

print("Data processing complete and saved to Google Bucket")
"""

    return code