import hashlib
import streamlit as st
from utils.database import get_db
from sqlalchemy import text

def hash_password(password: str) -> str:
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_user(username: str, password: str) -> bool:
    """Verify user credentials against database"""
    db = next(get_db())
    try:
        # Use text() for raw SQL and proper parameter binding
        result = db.execute(
            text("SELECT password_hash FROM users WHERE username = :username"),
            {"username": username}
        ).fetchone()

        if result and result[0] == hash_password(password):
            return True
        return False
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False
    finally:
        db.close()

def login_user():
    """Handle user login"""
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    if not st.session_state.logged_in:
        st.markdown("## 🔐 Login")

        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login")

            if submit:
                if verify_user(username, password):
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.success("Successfully logged in!")
                    st.rerun()
                else:
                    st.error("Invalid username or password")
                    return False
        return False
    return True

def logout_user():
    """Handle user logout"""
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = None
        st.rerun()