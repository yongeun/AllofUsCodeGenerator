import streamlit as st
import pandas as pd
from utils.code_templates import generate_python_code, generate_r_code
from utils.stats import preview_analysis
import base64

def main():
    st.set_page_config(page_title="Epidemiological Analysis Code Generator", layout="wide")
    
    # Custom CSS
    st.markdown("""
        <style>
            .main {
                padding: 2rem;
            }
            .stButton>button {
                width: 100%;
            }
        </style>
    """, unsafe_allow_html=True)
    
    st.title("Epidemiological Analysis Code Generator")
    
    # Sidebar for configuration
    st.sidebar.header("Analysis Configuration")
    language = st.sidebar.selectbox("Select Language", ["Python", "R"])
    
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
        
        # Generate code based on selected language
        if language == "Python":
            generated_code = generate_python_code(config)
        else:
            generated_code = generate_r_code(config)
        
        # Display generated code
        st.subheader("Generated Code")
        st.code(generated_code, language=language.lower())
        
        # Download button
        if language == "Python":
            file_extension = "py"
        else:
            file_extension = "R"
            
        file_name = f"analysis_code.{file_extension}"
        
        b64 = base64.b64encode(generated_code.encode()).decode()
        href = f'<a href="data:file/text;base64,{b64}" download="{file_name}">Download Generated Code</a>'
        st.markdown(href, unsafe_allow_html=True)
        
        # Preview Analysis section
        if st.checkbox("Preview Analysis Results"):
            preview_analysis(config)

if __name__ == "__main__":
    main()
