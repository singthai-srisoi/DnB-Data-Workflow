from database import get_db_session
from models.customer import Customer
import polars as pl
import streamlit as st
import fastexcel as fex
import polars as pl
from typing import List, Dict, Any

# TODO: Add function to import from sqlaccounting
# reset session state
st.session_state.clear()

# file = fex.read_excel(data_file_)
# sheets = file.sheet_names

def get_data_db():
    session = next(get_db_session())  # Get a session
    try:
        query = session.query(Customer).all()  # Fetch all results
        if not query:
            print("No data found.")
            return
        # Convert SQLAlchemy ORM objects to list of dictionaries
        data = [row.__dict__ for row in query]
        # Remove the `_sa_instance_state` key (internal SQLAlchemy attribute)
        for row in data:
            row.pop('_sa_instance_state', None)
        # Create a Polars DataFrame
        df = pl.DataFrame(data)
        return df
    finally:
        session.close()  # Always close the session

def pre_import_data_db(file: bytes, sheet_name: str = 'Maintain Customer') -> tuple[List[Customer], pl.DataFrame, pl.DataFrame]:
    session = next(get_db_session())
    df = pl.read_excel(file, sheet_name=sheet_name)
    rename_map = {
        'Code': 'code',
        'Control A/C': 'control_ac',
        'Company Name': 'company_name',
        '2nd Company Name': 'second_company_name',
        'Address 1': 'address_1',
        'Address 2': 'address_2',
        'Address 3': 'address_3',
        'Post Code': 'post_code',
        'TIN': 'tin',
        'ID Type': 'id_type',
        'ID No': 'id_no'
    }
    rename_map = {old: new for old, new in rename_map.items() if old in df.columns}
    # columns not int rename
    drop_column = [col for col in df.columns if col not in rename_map.keys()]
    df = df.drop(drop_column).rename(rename_map).drop_nulls(subset=['code', 'company_name'])
    customer_list: List[Customer] = []
    customer_upload: List[Dict[str, Any]] = []
    
    for row in df.to_dicts():
        check_exist = session.query(Customer).filter(Customer.code == row.get('code')).first()
        if check_exist:
            continue
        customer = Customer(**row)
        customer_list.append(customer)
        customer_upload.append(row)
    customer_upload_df = pl.DataFrame(customer_upload)
    return (customer_list, customer_upload_df, df)

def import_data_db(data: List[Customer]):
    session = next(get_db_session())
    try:
        session.add_all(data)
        session.commit()
        print(f"Succesfully imported {len(data)} records")
    finally:
        session.close()


st.title("Customer Import")
st.header("Data from Database")
df = get_data_db()

if "df" not in st.session_state:
    st.session_state.df = get_data_db()
if st.button("Refresh"):
    st.session_state.df = get_data_db()
if st.session_state.df is not None:
    st.dataframe(st.session_state.df)
else:
    st.write("No data found.")

# <hr />
st.divider()
st.header("Upload File")
st.write("Upload a file to import customer data.")
data_file = st.file_uploader("Upload a file", type=["xlsx"])

if "sheets_name" not in st.session_state:
    st.session_state.sheets_name = None
if "pre_upload_data" not in st.session_state:
    st.session_state.pre_upload_data = None
if data_file:
    # data_file to bytes
    data_file_ = data_file.read()
    sheets = fex.read_excel(data_file_).sheet_names
    st.text("select sheet name")
    st.session_state.sheets_name = st.selectbox("Sheet Name", sheets)
    
if st.session_state.sheets_name:
    st.session_state.pre_upload_data = pre_import_data_db(data_file_, st.session_state.sheets_name)
if st.session_state.pre_upload_data is not None:
    if st.session_state.pre_upload_data[2] is not None and not st.session_state.pre_upload_data[2].is_empty():
        st.header("Preview Data")
        st.dataframe(st.session_state.pre_upload_data[2])
        st.header("Data To Be upload")
        if not st.session_state.pre_upload_data[1].is_empty():
            st.dataframe(st.session_state.pre_upload_data[1])
        st.text(f" Total record to be uploaded: {len(st.session_state.pre_upload_data[0])}")
        if st.button("Import Data") and len(st.session_state.pre_upload_data[0]) > 0:
            import_data_db(st.session_state.pre_upload_data[0])
            st.session_state.pre_upload_data = None
            st.session_state.sheets_name = None
            st.session_state.df = get_data_db()
            st.write("Data imported successfully.")
