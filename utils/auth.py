import bcrypt
import streamlit as st
from utils.database import get_db
from sqlalchemy import text

def hash_password(password: str) -> str:
    """Hash password using bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed_password: str) -> bool:
    """Verify password against hashed password"""
    try:
        return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception as e:
        st.error(f"Password verification error: {str(e)}")
        return False

def create_demo_user(db):
    """Create demo user if it doesn't exist"""
    try:
        password = "demo123"
        hashed_password = hash_password(password)

        db.execute(
            text("""
                INSERT INTO users (username, password_hash, email)
                VALUES (:username, :password_hash, :email)
                ON CONFLICT (username) DO UPDATE
                SET password_hash = :password_hash
            """),
            {
                "username": "demo_user",
                "password_hash": hashed_password,
                "email": "demo@example.com"
            }
        )
        db.commit()
    except Exception as e:
        st.error(f"Error creating demo user: {str(e)}")

def verify_user(username: str, password: str) -> bool:
    """Verify user credentials against database"""
    db = next(get_db())
    try:
        # First ensure demo user exists
        create_demo_user(db)

        # Then verify credentials
        result = db.execute(
            text("SELECT password_hash FROM users WHERE username = :username"),
            {"username": username}
        ).fetchone()

        if result:
            return verify_password(password, result[0])
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