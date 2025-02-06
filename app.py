import streamlit as st
import pandas as pd
from utils.code_templates import generate_python_code, generate_r_code
from utils.stats import preview_analysis
from utils.database import get_db, Analysis
import base64
from contextlib import contextmanager
from sqlalchemy.orm import Session

def get_db_session():
    """Get database session context manager"""
    session = next(get_db())
    try:
        yield session
    finally:
        session.close()

def save_analysis(config, python_code, r_code, description=""):
    """Save analysis configuration and generated code to database"""
    with contextmanager(get_db_session)() as db:
        analysis = Analysis(
            config=config,
            python_code=python_code,
            r_code=r_code,
            description=description
        )
        db.add(analysis)
        db.commit()
        return analysis.id

def create_download_link(code, filename):
    """Create a download link for code files"""
    b64 = base64.b64encode(code.encode()).decode()
    href = f'<a href="data:file/text;base64,{b64}" download="{filename}">📥 Download {filename}</a>'
    return href

def main():
    st.set_page_config(
        page_title="Epidemiological Analysis Code Generator",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS
    with open('assets/style.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    # Header with emoji and description
    st.markdown("""
        <h1>🧬 Epidemiological Analysis Code Generator</h1>
        <p class="subtitle">Generate customized Python and R code for epidemiological studies using EHR data</p>
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
        analysis_id = save_analysis(
            config=config,
            python_code=python_code,
            r_code=r_code,
            description=description
        )

        st.success(f"✅ Analysis saved successfully with ID: {analysis_id}")

        # Display Python code
        st.markdown("### 🐍 1. Python Code (Data Preparation)")
        st.code(python_code, language="python")
        st.markdown(create_download_link(python_code, "data_preparation.py"), unsafe_allow_html=True)

        # Display R code
        st.markdown("### 📊 2. R Code (Statistical Analysis)")
        st.code(r_code, language="r")
        st.markdown(create_download_link(r_code, "statistical_analysis.R"), unsafe_allow_html=True)

        # Preview Analysis section
        if st.checkbox("🔍 Preview Analysis Results"):
            preview_analysis(config)

if __name__ == "__main__":
    main()