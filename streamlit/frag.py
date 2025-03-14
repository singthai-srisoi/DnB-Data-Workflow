import streamlit as st
import time

@st.fragment()
def toggle_and_text():
    cols = st.columns(2)
    cols[0].toggle(f"Toggle {st.session_state.clicked}")
    cols[1].text_area("Enter text")

if "clicked" not in st.session_state:
    st.session_state.clicked = 0
toggle_and_text()
if st.button("Click me"):
    st.session_state.clicked += 1
    st.write(f"Clicked {st.session_state.clicked} times.")