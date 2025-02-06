from utils.python_templates import get_python_template
from utils.r_templates import get_r_template

def generate_python_code(config):
    """Generate Python code based on configuration"""
    return get_python_template(config)

def generate_r_code(config):
    """Generate R code based on configuration"""
    return get_r_template(config)
