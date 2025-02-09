import streamlit as st
import pandas as pd
from utils.code_templates import generate_python_code, generate_r_code
from utils.database import get_db, Analysis
import base64
from contextlib import contextmanager
from sqlalchemy.orm import Session
import logging
from sqlalchemy.exc import SQLAlchemyError

# Set up logging
logger = logging.getLogger(__name__)

def get_db_session():
    """Get database session context manager with error handling"""
    try:
        session = next(get_db())
        logger.debug("Database session started")
        yield session
    except Exception as e:
        logger.error(f"Failed to get database session: {e}")
        raise
    finally:
        session.close()
        logger.debug("Database session closed")

def save_analysis(config, python_code, r_code, description=""):
    """Save analysis configuration and generated code to database"""
    try:
        with contextmanager(get_db_session)() as db:
            analysis = Analysis(
                config=config,
                python_code=python_code,
                r_code=r_code,
                description=description
            )
            db.add(analysis)
            db.commit()
            logger.info(f"Analysis saved successfully with ID: {analysis.id}")
            return analysis.id
    except SQLAlchemyError as e:
        logger.error(f"Database error while saving analysis: {e}")
        st.error("Failed to save analysis to database. Please try again.")
        db.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error while saving analysis: {e}")
        st.error("An unexpected error occurred. Please try again.")
        raise

def create_download_link(code, filename):
    """Create a download link for code files"""
    b64 = base64.b64encode(code.encode()).decode()
    href = f'<a href="data:file/text;base64,{b64}" download="{filename}">📥 Download {filename}</a>'
    return href

def create_input_form():
    st.write("### Configure Analysis Parameters")
    
    # Exposure Variable Configuration
    st.write("#### Exposure Variable")
    exposure_type = st.selectbox(
        "Select Exposure Type",
        ["Condition (ICD Codes)", "Medication"],
        key="exposure_type"
    )
    
    exposure_codes = {}
    if exposure_type == "Condition (ICD Codes)":
        exposure_icd9 = st.text_area(
            "Enter Exposure ICD-9 Codes (comma-separated)",
            help="Example: 250.00, 250.01, 250.02",
            key="exposure_icd9"
        )
        exposure_icd10 = st.text_area(
            "Enter Exposure ICD-10 Codes (comma-separated)",
            help="Example: E11.9, E11.65, E11.01",
            key="exposure_icd10"
        )
        
        # Validate exposure codes
        exposure_icd9_list = [code.strip() for code in exposure_icd9.split(",")] if exposure_icd9 else []
        exposure_icd10_list = [code.strip() for code in exposure_icd10.split(",")] if exposure_icd10 else []
        
        if exposure_icd9:
            invalid_icd9 = validate_codes(exposure_icd9_list, "ICD9")
            if invalid_icd9:
                st.error(f"Invalid exposure ICD-9 codes: {', '.join(invalid_icd9)}")
        
        if exposure_icd10:
            invalid_icd10 = validate_codes(exposure_icd10_list, "ICD10")
            if invalid_icd10:
                st.error(f"Invalid exposure ICD-10 codes: {', '.join(invalid_icd10)}")
        
        exposure_codes = {
            "type": "condition",
            "icd9": exposure_icd9_list,
            "icd10": exposure_icd10_list
        }
    else:  # Medication
        exposure_meds = st.text_area(
            "Enter Exposure Medication Names (comma-separated)",
            help="Example: metformin, glipizide, sitagliptin",
            key="exposure_meds"
        )
        exposure_codes = {
            "type": "medication",
            "names": [med.strip().lower() for med in exposure_meds.split(",")] if exposure_meds else []
        }

    # Outcome Variable Configuration
    st.write("#### Outcome Variable")
    outcome_type = st.selectbox(
        "Select Outcome Type",
        ["Condition (ICD Codes)", "Medication"],
        key="outcome_type"
    )
    
    outcome_codes = {}
    if outcome_type == "Condition (ICD Codes)":
        outcome_icd9 = st.text_area(
            "Enter Outcome ICD-9 Codes (comma-separated)",
            help="Example: 571.5",
            key="outcome_icd9"
        )
        outcome_icd10 = st.text_area(
            "Enter Outcome ICD-10 Codes (comma-separated)",
            help="Example: K75.8, K76.0",
            key="outcome_icd10"
        )
        
        # Validate outcome codes
        outcome_icd9_list = [code.strip() for code in outcome_icd9.split(",")] if outcome_icd9 else []
        outcome_icd10_list = [code.strip() for code in outcome_icd10.split(",")] if outcome_icd10 else []
        
        if outcome_icd9:
            invalid_icd9 = validate_codes(outcome_icd9_list, "ICD9")
            if invalid_icd9:
                st.error(f"Invalid outcome ICD-9 codes: {', '.join(invalid_icd9)}")
        
        if outcome_icd10:
            invalid_icd10 = validate_codes(outcome_icd10_list, "ICD10")
            if invalid_icd10:
                st.error(f"Invalid outcome ICD-10 codes: {', '.join(invalid_icd10)}")
        
        outcome_codes = {
            "type": "condition",
            "icd9": outcome_icd9_list,
            "icd10": outcome_icd10_list
        }
    else:  # Medication
        outcome_meds = st.text_area(
            "Enter Outcome Medication Names (comma-separated)",
            help="Example: atorvastatin, simvastatin",
            key="outcome_meds"
        )
        outcome_codes = {
            "type": "medication",
            "names": [med.strip().lower() for med in outcome_meds.split(",")] if outcome_meds else []
        }

    description = st.text_area("Description (optional)", help="Add notes about this analysis")
    
    return {
        "exposure": exposure_codes,
        "outcome": outcome_codes,
        "description": description
    }

def main():
    st.set_page_config(
        page_title="All of Us Research Program Analysis Code Generator",
        layout="wide",
        initial_sidebar_state="expanded"
    )


    # Custom CSS
    with open('assets/style.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    # Header with emoji and description
    st.markdown("""
        <h1>All of Us Research Program Analysis Code Generator</h1> 
        <p class="subtitle">Generate customized Python and R code for studies using EHR data</p>
    """, unsafe_allow_html=True)

    # Main form
    with st.form("analysis_form"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 📊 Exposure Variables")
            exposure_type = st.selectbox(
                "Exposure Variable Type",
                ["Condition", "Medication", "Procedure"],
                key="exposure_type"
            )
            exposure_var = st.text_input("Exposure Variable SNOMED Code", "44823375")

            st.markdown("### 🔄 Confounding Factors")
            include_age = st.checkbox("Age", value=True)
            include_sex = st.checkbox("Sex", value=True)
            include_race_ethnicity = st.checkbox("Race/Ethnicity", value=True)
            include_insurance = st.checkbox("Insurance Status", value=True)
            include_income = st.checkbox("Income Status", value=True)
            include_education = st.checkbox("Education Status", value=True)
            include_smoking = st.checkbox("Active Smoking", value=True)

        with col2:
            st.markdown("### 📈 Outcome Variables")
            outcome_type = st.selectbox(
                "Outcome Variable Type",
                ["Condition", "Medication", "Procedure"],
                key="outcome_type"
            )
            outcome_var = st.text_input("Outcome Variable SNOMED Code", "35683383")

            st.markdown("### ⛔ Exclusion Criteria")
            exclusion_type = st.selectbox(
                "Exclusion Variable Type",
                ["None", "Condition", "Medication", "Procedure"],
                key="exclusion_type"
            )
            exclusion_var = st.text_input("Exclusion Variable SNOMED Code (optional)", "")

            st.markdown("### ⚙️ Additional Settings")
            include_visualization = st.checkbox("Include Visualizations", value=True)
            include_advanced_stats = st.checkbox("Include Advanced Statistics", value=True)

        description = st.text_area("📝 Analysis Description", placeholder="Enter a description of your analysis...")
        submitted = st.form_submit_button("🚀 Generate Code")

    if submitted:
        # Create configuration dictionary
        config = {
            "exposure_var": [int(exposure_var)],
            "exposure_type": exposure_type.lower(),
            "outcome_var": [int(outcome_var)],
            "outcome_type": outcome_type.lower(),
            "exclusion_var": [int(exclusion_var)] if exclusion_var and exclusion_type != "None" else [],
            "exclusion_type": exclusion_type.lower() if exclusion_type != "None" else None,
            "confounders": {
                "age": include_age,
                "sex": include_sex,
                "race_ethnicity": include_race_ethnicity,
                "insurance": include_insurance,
                "income": include_income,
                "education": include_education,
                "smoking": include_smoking
            },
            "include_visualization": include_visualization,
            "include_advanced_stats": include_advanced_stats
        }

        # Generate both Python and R code
        python_code = generate_python_code(config)
        r_code = generate_r_code(config)

        # Save analysis to database
        try:
            analysis_id = save_analysis(
                config=config,
                python_code=python_code,
                r_code=r_code,
                description=description
            )
            st.success(f"✅ Analysis saved successfully with ID: {analysis_id}")
        except Exception as e:
            st.error("Failed to save analysis. Please try again.")
            logger.error(f"Error in main function: {e}")

        # Display Python code
        st.markdown("### 🐍 1. Python Code (Data Preparation)")
        st.code(python_code, language="python")
        st.markdown(create_download_link(python_code, "data_preparation.py"), unsafe_allow_html=True)

        # Display R code
        st.markdown("### 📊 2. R Code (Statistical Analysis)")
        st.code(r_code, language="r")
        st.markdown(create_download_link(r_code, "statistical_analysis.R"), unsafe_allow_html=True)

if __name__ == "__main__":
    main()