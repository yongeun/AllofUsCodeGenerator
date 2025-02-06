import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

def get_auth_config():
    """Get authentication configuration"""
    # Default configuration with a sample user
    passwords = ['demo123']  # List of passwords to hash
    hashed_passwords = stauth.Hasher(passwords).generate()

    config = {
        'credentials': {
            'usernames': {
                'demo_user': {
                    'email': 'demo@example.com',
                    'name': 'Demo User',
                    'password': hashed_passwords[0]  # Use the first hashed password
                }
            }
        },
        'cookie': {
            'expiry_days': 30,
            'key': 'some_signature_key',
            'name': 'epidemiology_analysis_cookie'
        }
    }

    # Try to load existing configuration
    try:
        with open('.streamlit/auth_config.yaml') as file:
            config = yaml.load(file, Loader=SafeLoader)
    except FileNotFoundError:
        # Save default configuration
        with open('.streamlit/auth_config.yaml', 'w') as file:
            yaml.dump(config, file, default_flow_style=False)

    return config