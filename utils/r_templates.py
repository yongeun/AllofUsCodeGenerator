def get_r_template(config):
    """Generate R code template"""
    
    code = f"""library(tidyverse)
library(finalfit)
library(googleCloudStorageR)
library(haven)

# Configuration
exposure_vars <- c({', '.join(map(str, config['exposure_var']))})
outcome_vars <- c({', '.join(map(str, config['outcome_var']))})

# Set up Google Cloud Storage
gcs_global_bucket(Sys.getenv("WORKSPACE_BUCKET"))

# Load data
load_data <- function() {{
  temp_file <- tempfile(fileext = ".csv")
  gcs_get_object("data/ehr_df.csv", saveToDisk = temp_file)
  df <- read_csv(temp_file)
  unlink(temp_file)
  return(df)
}}

# Preprocess data
preprocess_data <- function(df) {{
  df %>%
    mutate(
      across(c(race_cat, ethnicity_cat, sex_cat), as.factor),
      var_1 = as.factor(var_1),
      var_2 = as.factor(var_2)
    )
}}

# Perform analysis
perform_analysis <- function(df) {{
  # Summary statistics
  summary_stats <- df %>%
    summary_factorlist(dependent = "var_2",
                      explanatory = c("var_1", "age", "sex_cat", "race_cat", "ethnicity_cat"))
  print(summary_stats)
  
  # Logistic regression
  model <- df %>%
    finalfit(dependent = "var_2",
            explanatory = c("var_1", "age", "sex_cat", "race_cat", "ethnicity_cat"))
  print(model)
}}
"""
    
    if config['include_visualization']:
        code += """
# Create visualizations
create_visualizations <- function(df) {
  # Create visualization directory
  dir.create("visualizations", showWarnings = FALSE)
  
  # Age distribution plot
  pdf("visualizations/age_distribution.pdf")
  ggplot(df, aes(x = age, fill = var_2)) +
    geom_histogram(position = "stack") +
    theme_minimal() +
    labs(title = "Age Distribution by Outcome")
  dev.off()
  
  # Outcome by exposure
  pdf("visualizations/outcome_by_exposure.pdf")
  ggplot(df, aes(x = var_1, fill = var_2)) +
    geom_bar(position = "dodge") +
    theme_minimal() +
    labs(title = "Outcome by Exposure")
  dev.off()
}
"""
    
    code += """
# Main execution
main <- function() {
  df <- load_data()
  df <- preprocess_data(df)
  perform_analysis(df)
"""
    
    if config['include_visualization']:
        code += """  create_visualizations(df)
"""
    
    code += """}

main()
"""
    
    return code
