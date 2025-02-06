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
    href = f'<a href="data:file/text;base64,{b64}" download="{filename}">Download {filename}</a>'
    return href

def main():
    st.set_page_config(page_title="Epidemiological Analysis Code Generator", layout="wide")

    # Add clipboard.js to the page
    st.markdown("""
        <script src="https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.8/clipboard.min.js"></script>
        <script>
        document.addEventListener('DOMContentLoaded', function() {
            var clipboardPython = new ClipboardJS('#copy-python');
            var clipboardR = new ClipboardJS('#copy-r');

            clipboardPython.on('success', function() {
                document.getElementById('copy-python').innerHTML = 'Copied!';
                setTimeout(function() {
                    document.getElementById('copy-python').innerHTML = 'Copy Python Code';
                }, 2000);
            });

            clipboardR.on('success', function() {
                document.getElementById('copy-r').innerHTML = 'Copied!';
                setTimeout(function() {
                    document.getElementById('copy-r').innerHTML = 'Copy R Code';
                }, 2000);
            });
        });
        </script>
    """, unsafe_allow_html=True)

    # Custom CSS
    st.markdown("""
        <style>
            .main {
                padding: 2rem;
            }
            .stButton>button {
                width: 100%;
            }
            .copy-button {
                background-color: #1f77b4;
                color: white;
                padding: 0.5rem 1rem;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                margin: 0.5rem 0;
                font-size: 0.9rem;
            }
            .copy-button:hover {
                background-color: #145c8e;
            }
            .code-container {
                position: relative;
                margin: 1rem 0;
            }
            .code-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 0.5rem;
            }
            .code-buttons {
                display: flex;
                gap: 1rem;
            }
        </style>
    """, unsafe_allow_html=True)

    st.title("Epidemiological Analysis Code Generator")

    # Main form
    with st.form("analysis_form"):
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Exposure Variables")
            exposure_var = st.text_input("Exposure Variable SNOMED Code", "44823375")

            st.subheader("Confounding Factors")
            include_age = st.checkbox("Age", value=True)
            include_sex = st.checkbox("Sex", value=True)
            include_race = st.checkbox("Race", value=True)
            include_ethnicity = st.checkbox("Ethnicity", value=True)

        with col2:
            st.subheader("Outcome Variables")
            outcome_var = st.text_input("Outcome Variable SNOMED Code", "35683383")

            st.subheader("Additional Settings")
            include_visualization = st.checkbox("Include Visualizations", value=True)
            include_advanced_stats = st.checkbox("Include Advanced Statistics", value=True)

        description = st.text_area("Analysis Description", "")
        submitted = st.form_submit_button("Generate Code")

    if submitted:
        # Create configuration dictionary
        config = {
            "exposure_var": [int(exposure_var)],
            "outcome_var": [int(outcome_var)],
            "confounders": {
                "age": include_age,
                "sex": include_sex,
                "race": include_race,
                "ethnicity": include_ethnicity
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

        st.success(f"Analysis saved with ID: {analysis_id}")

        # Display Python code with copy button
        st.markdown("""
            <div class="code-container">
                <div class="code-header">
                    <h3>1. Python Code (Data Preparation)</h3>
                    <div class="code-buttons">
                        <button class="copy-button" id="copy-python" data-clipboard-target="#python-code">
                            Copy Python Code
                        </button>
                        {}
                    </div>
                </div>
            </div>
        """.format(create_download_link(python_code, "data_preparation.py")), unsafe_allow_html=True)

        st.code(python_code, language="python", key="python-code")

        # Display R code with copy button
        st.markdown("""
            <div class="code-container">
                <div class="code-header">
                    <h3>2. R Code (Statistical Analysis)</h3>
                    <div class="code-buttons">
                        <button class="copy-button" id="copy-r" data-clipboard-target="#r-code">
                            Copy R Code
                        </button>
                        {}
                    </div>
                </div>
            </div>
        """.format(create_download_link(r_code, "statistical_analysis.R")), unsafe_allow_html=True)

        st.code(r_code, language="r", key="r-code")

        # Preview Analysis section
        if st.checkbox("Preview Analysis Results"):
            preview_analysis(config)

if __name__ == "__main__":
    main()