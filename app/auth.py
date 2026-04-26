"""Email + password gate for the Sourcing Assistant.

Allowed emails come from Streamlit secrets `allowed_emails` (comma-separated
or list). The shared password comes from `app_password`. Both are checked.
"""
import streamlit as st


def _get_secret(key, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def login_gate():
    expected_pw = _get_secret('app_password', None)
    raw_emails = _get_secret('allowed_emails', None)
    if isinstance(raw_emails, str):
        allowed = {e.strip().lower() for e in raw_emails.split(',') if e.strip()}
    elif isinstance(raw_emails, (list, tuple)):
        allowed = {str(e).strip().lower() for e in raw_emails if str(e).strip()}
    else:
        allowed = set()

    # Auth disabled if either is missing (handy for local dev)
    if not expected_pw or not allowed:
        return

    if st.session_state.get('authed'):
        return

    st.markdown("# 🎯 Sourcing Assistant")
    st.caption("Internal tool — sign in to continue.")
    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("Email", placeholder="you@example.com").strip().lower()
        pw = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in", type="primary")
    if submitted:
        if email in allowed and pw == expected_pw:
            st.session_state['authed'] = True
            st.session_state['user_email'] = email
            st.rerun()
        else:
            st.error("Wrong email or password.")
    st.stop()
