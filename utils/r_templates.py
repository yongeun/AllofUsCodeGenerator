def get_r_template(config):
    """Generate R code template for statistical analysis"""

    # Build list of explanatory variables based on selected confounders
    explanatory_vars = []
    if config['confounders']['age']:
        explanatory_vars.extend(['age', 'age_group_code'])
    if config['confounders']['sex']:
        explanatory_vars.append('sex_cat')
    if config['confounders']['race_ethnicity']:
        explanatory_vars.extend(['race_cat', 'ethnicity_cat'])
    if config['confounders']['insurance']:
        explanatory_vars.append('insurance_status')
    if config['confounders']['income']:
        explanatory_vars.append('income_status')
    if config['confounders']['education']:
        explanatory_vars.append('education_status')
    if config['confounders']['smoking']:
        explanatory_vars.append('active_smoking')

    # Convert list to R vector string
    explanatory_vars_str = '", "'.join(explanatory_vars)

    code = """install.packages("finalfit")
library("finalfit")
library("tidyverse")

# This code copies a file from your Google Bucket into a dataframe
name_of_file_in_bucket <- 'ehr_df.csv'

########################################################################
##
################# DON'T CHANGE FROM HERE ###############################
##
########################################################################

# Get the bucket name
my_bucket <- Sys.getenv('WORKSPACE_BUCKET')

# Copy the file from current workspace to the bucket
system(paste0("gsutil cp ", my_bucket, "/data/", name_of_file_in_bucket, " ."), intern=TRUE)

# Load the file into a dataframe
ehr_df <- read_csv(name_of_file_in_bucket)
head(ehr_df)

# Convert variables to factors
# Demographic factors
ehr_df$age_group_code <- as.factor(ehr_df$age_group_code)
ehr_df$race_cat <- as.factor(ehr_df$race_cat)
ehr_df$ethnicity_cat <- as.factor(ehr_df$ethnicity_cat)
ehr_df$sex_cat <- as.factor(ehr_df$sex_cat)

# Socioeconomic factors
ehr_df$insurance_status <- as.factor(ehr_df$insurance_status)
ehr_df$income_status <- as.factor(ehr_df$income_status)
ehr_df$education_status <- as.factor(ehr_df$education_status)

# Health behaviors
ehr_df$active_smoking <- as.factor(ehr_df$active_smoking)

# Main variables
ehr_df$var_1 <- as.factor(ehr_df$var_1)
ehr_df$var_2 <- as.factor(ehr_df$var_2)

# Univariable analysis for exposure variable
explanatory <- c("%s")
dependent <- "var_1"
ehr_df %>%
    summary_factorlist(dependent, explanatory, p=TRUE, na_include=TRUE)

# Multivariable analysis for outcome
explanatory <- c("var_1", "%s")
dependent <- "var_2"
ehr_df %>% 
    finalfit(dependent, explanatory, dependent_label_prefix = "")
""" % (explanatory_vars_str, explanatory_vars_str)

    return code