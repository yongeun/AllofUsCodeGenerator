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

def create_user(db, email: str, password: str, username: str = None):
    """Create a new user"""
    try:
        # Check if email already exists
        result = db.execute(
            text("SELECT email FROM users WHERE email = :email"),
            {"email": email}
        ).fetchone()

        if result:
            return False, "Email already registered"

        hashed_password = hash_password(password)

        # Use email as username if none provided
        if not username:
            username = email.split('@')[0]

        db.execute(
            text("""
                INSERT INTO users (username, email, password_hash)
                VALUES (:username, :email, :password_hash)
            """),
            {
                "username": username,
                "email": email,
                "password_hash": hashed_password
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
                INSERT INTO users (username, email, password_hash)
                VALUES (:username, :email, :password_hash)
                ON CONFLICT (email) DO UPDATE
                SET password_hash = :password_hash
            """),
            {
                "username": "demo_user",
                "email": "demo@example.com",
                "password_hash": hashed_password
            }
        )
        db.commit()
    except Exception as e:
        st.error(f"Error creating demo user: {str(e)}")

def verify_user(email: str, password: str) -> bool:
    """Verify user credentials against database"""
    db = next(get_db())
    try:
        # First ensure demo user exists
        create_demo_user(db)

        # Then verify credentials
        result = db.execute(
            text("SELECT password_hash FROM users WHERE email = :email"),
            {"email": email}
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
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                submit = st.form_submit_button("Login")

                if submit:
                    if verify_user(email, password):
                        st.session_state.logged_in = True
                        st.session_state.username = email.split('@')[0]  # Use email prefix as display name
                        st.success("Successfully logged in!")
                        st.rerun()
                    else:
                        st.error("Invalid email or password")
                        return False

        with tab2:
            with st.form("signup_form"):
                new_email = st.text_input("Email")
                username = st.text_input("Display Name (optional)")
                new_password = st.text_input("Choose Password", type="password")
                confirm_password = st.text_input("Confirm Password", type="password")
                signup_submit = st.form_submit_button("Sign Up")

                if signup_submit:
                    if not new_email or not new_password:
                        st.error("Email and password are required")
                        return False
                    if not '@' in new_email or not '.' in new_email:
                        st.error("Please enter a valid email address")
                        return False
                    if new_password != confirm_password:
                        st.error("Passwords do not match")
                        return False
                    if len(new_password) < 6:
                        st.error("Password must be at least 6 characters long")
                        return False

                    db = next(get_db())
                    success, message = create_user(db, new_email, new_password, username)
                    if success:
                        st.success(message)
                        st.info("Please log in with your email and password")
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