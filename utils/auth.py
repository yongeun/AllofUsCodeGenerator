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

def create_user(db, username: str, password: str, email: str = None):
    """Create a new user"""
    try:
        # Check if username already exists
        result = db.execute(
            text("SELECT username FROM users WHERE username = :username"),
            {"username": username}
        ).fetchone()

        if result:
            return False, "Username already exists"

        hashed_password = hash_password(password)

        db.execute(
            text("""
                INSERT INTO users (username, password_hash, email)
                VALUES (:username, :password_hash, :email)
            """),
            {
                "username": username,
                "password_hash": hashed_password,
                "email": email
            }
        )
        db.commit()
        return True, "User created successfully"
    except Exception as e:
        st.error(f"Error creating user: {str(e)}")
        return False, f"Error creating user: {str(e)}"

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

        # Create tabs for login and signup
        tab1, tab2 = st.tabs(["Login", "Sign Up"])

        with tab1:
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

        with tab2:
            with st.form("signup_form"):
                new_username = st.text_input("Choose Username")
                new_password = st.text_input("Choose Password", type="password")
                confirm_password = st.text_input("Confirm Password", type="password")
                email = st.text_input("Email (optional)")
                signup_submit = st.form_submit_button("Sign Up")

                if signup_submit:
                    if not new_username or not new_password:
                        st.error("Username and password are required")
                        return False
                    if new_password != confirm_password:
                        st.error("Passwords do not match")
                        return False
                    if len(new_password) < 6:
                        st.error("Password must be at least 6 characters long")
                        return False

                    db = next(get_db())
                    success, message = create_user(db, new_username, new_password, email)
                    if success:
                        st.success(message)
                        st.info("Please log in with your new credentials")
                    else:
                        st.error(message)
                    db.close()
        return False
    return True

def logout_user():
    """Handle user logout"""
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = None
        st.rerun()