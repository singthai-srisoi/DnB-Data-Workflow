import streamlit as st
import polars as pl
import fastexcel as fex
from typing import List, Dict, Any
from database import get_db_session
from models.supplier import Supplier
from services.user import UserService
from sqlacc import ComServerService, get_comserver

# --- Streamlit App Setup ---
# reset session state
st.session_state.clear()
st.title("Supplier Import")

# --- Functions ---
def get_data_db():
    session = next(get_db_session())
    try:
        query = session.query(Supplier).all()
        if not query:
            return None
        data = [row.__dict__ for row in query]
        for row in data:
            row.pop('_sa_instance_state', None)
        return pl.DataFrame(data)
    finally:
        session.close()

def pre_import_data_db(file: bytes, sheet_name: str = 'Maintain Supplier') -> tuple[List[Supplier], pl.DataFrame, pl.DataFrame]:
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
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    drop_column = [col for col in df.columns if col not in rename_map.keys()]
    df = df.drop(drop_column).rename(rename_map).drop_nulls(subset=['code', 'company_name'])

    supplier_list: List[Supplier] = []
    supplier_upload: List[Dict[str, Any]] = []

    for row in df.to_dicts():
        check_exist = session.query(Supplier).filter(Supplier.code == row.get('code')).first()
        if check_exist:
            continue
        supplier = Supplier(**row)
        supplier_list.append(supplier)
        supplier_upload.append(row)

    return (supplier_list, pl.DataFrame(supplier_upload), df)

def import_data_db(data: List[Supplier]):
    session = next(get_db_session())
    try:
        session.add_all(data)
        session.commit()
        st.success(f"Successfully imported {len(data)} records.")
    finally:
        session.close()

def import_from_sqlacc():
    session = next(get_db_session())
    try:
        com_server = get_comserver()
        UserService.insert_unimported(session, com_server, model='AP_SUPPLIER')
        st.success("Data imported from SQLAcc successfully.")
    except Exception as e:
        st.error(f"Failed to import from SQLAcc: {e}")
    finally:
        session.close()

# --- UI State Setup ---
if "df" not in st.session_state:
    st.session_state.df = get_data_db()
if "sheets_name" not in st.session_state:
    st.session_state.sheets_name = None
if "pre_upload_data" not in st.session_state:
    st.session_state.pre_upload_data = None

# --- Data Viewer ---
st.header("Data in Database")
if st.button("Refresh"):
    st.session_state.df = get_data_db()
if st.session_state.df is not None:
    st.dataframe(st.session_state.df)
else:
    st.write("No data found.")

# --- Import Source Selector ---
st.divider()
import_source = st.radio("Select import source", ["Upload File", "SQLAcc Direct Import"])
st.divider()

# --- Upload File Mode ---
if import_source == "Upload File":
    st.subheader("Upload a file to import supplier data.")
    data_file = st.file_uploader("Upload a file", type=["xlsx"])

    if data_file:
        data_file_ = data_file.read()
        sheets = fex.read_excel(data_file_).sheet_names
        st.session_state.sheets_name = st.selectbox("Sheet Name", sheets)

    if st.session_state.sheets_name:
        st.session_state.pre_upload_data = pre_import_data_db(data_file_, st.session_state.sheets_name)

    if st.session_state.pre_upload_data:
        preview_df = st.session_state.pre_upload_data[2]
        to_upload_df = st.session_state.pre_upload_data[1]
        supplier_objects = st.session_state.pre_upload_data[0]

        if not preview_df.is_empty():
            st.subheader("Preview Data")
            st.dataframe(preview_df)

            st.subheader("Data To Be Uploaded")
            if not to_upload_df.is_empty():
                st.dataframe(to_upload_df)

            st.text(f"Total records to upload: {len(supplier_objects)}")
            if st.button("Import Data"):
                import_data_db(supplier_objects)
                st.session_state.pre_upload_data = None
                st.session_state.sheets_name = None
                st.session_state.df = get_data_db()

# --- SQLAcc Direct Import Mode ---
elif import_source == "SQLAcc Direct Import":
    # if st.button("Import From SQLAcc"):
    #     import_from_sqlacc()
    #     st.session_state.df = get_data_db()
    st.subheader("Preview Unimported Supplier Codes from SQLAcc")

    if "unimported_codes" not in st.session_state:
        st.session_state.unimported_codes = None

    if st.button("Check Unimported Codes"):
        session = next(get_db_session())
        try:
            com_server = get_comserver()
            unimported = UserService.compare(session, com_server, model='AP_SUPPLIER')
            st.session_state.unimported_codes = unimported
        except Exception as e:
            st.error(f"Error while comparing: {e}")
        finally:
            session.close()

    if st.session_state.unimported_codes is not None:
        if len(st.session_state.unimported_codes) == 0:
            st.info("All supplier records from SQLAcc are already in the database.")
        else:
            st.write(f"Unimported Supplier Codes ({len(st.session_state.unimported_codes)}):")
            st.code("\n".join(st.session_state.unimported_codes))

            if st.button("Import From SQLAcc"):
                import_from_sqlacc()
                st.session_state.df = get_data_db()
                st.session_state.unimported_codes = None
