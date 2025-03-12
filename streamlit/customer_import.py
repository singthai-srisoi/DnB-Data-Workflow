from database import get_db_session
from models.customer import Customer
import polars as pl
import streamlit as st

def get_customer_from_db():
    with get_db_session() as session:
        customers = session.query(Customer).all()
    return customers
