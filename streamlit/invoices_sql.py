import streamlit as st
import polars as pl
import fastexcel as fex
import xlsxwriter

from database import get_db_session
from models import Supplier, Customer
from models.settings import Setting
from services.invoices_sql_service import Invoiceservice
import io

# region: Preload Page
# get supplier and customer data from database
session = next(get_db_session())
customers = session.query(Customer).all()
customers = [row.__dict__ for row in customers]
suppliers = session.query(Supplier).all()
suppliers = [row.__dict__ for row in suppliers]
setting: Setting = session.query(Setting).first()

if "df_customer" not in st.session_state:
    st.session_state.df_customer = pl.DataFrame(customers).drop(['_sa_instance_state'])
if "df_supplier" not in st.session_state:
    st.session_state.df_supplier = pl.DataFrame(suppliers).drop(['_sa_instance_state'])

# endregion


# region: Streamlit Page

st.title("Purchase Invoice Data Migration")
st.header("Upload File")
data_file = st.file_uploader("Upload a file", type=["xlsx"])

if "selected_sheet" not in st.session_state:
    st.session_state.selected_sheet = None
if "sal_pur" not in st.session_state:
    st.session_state.sal_pur = None
if "sal_grouped" not in st.session_state:
    st.session_state.sal_grouped = None
if "pur_grouped" not in st.session_state:
    st.session_state.pur_grouped = None
if "sal_unprocessed" not in st.session_state:
    st.session_state.sal_unprocessed = None
if "pur_unprocessed" not in st.session_state:
    st.session_state.pur_unprocessed = None


if data_file:
    # data_file to bytes
    data_file_ = data_file.read()
    sheets = fex.read_excel(data_file_).sheet_names
    st.text("select sheet name")
    sheet_name = st.selectbox("Sheet Name", sheets)
    
    if sheet_name:
        st.session_state.selected_sheet = sheet_name

if st.session_state.selected_sheet:
    # read into dataframe
    sal_pur = pl.read_excel(
        data_file_,
        sheet_name=st.session_state.selected_sheet,
    )
    sal_pur = sal_pur.rename({col: Invoiceservice.clean_column(col) for col in sal_pur.columns})
    
    st.session_state.sal_pur = sal_pur
    

def export_excel():
    pur_frac = Invoiceservice.fraction_df(
        st.session_state.pur_grouped,
        "Code"
    )
    sal_frac = Invoiceservice.fraction_df(
        st.session_state.sal_grouped,
        "Code"
    )
    stream = io.BytesIO()
    with xlsxwriter.Workbook(stream, {'in_memory': True}) as workbook:
        for i, df in enumerate(pur_frac):
            worksheet = workbook.add_worksheet(f"purchases_{i}")
            for col_idx, col_name in enumerate(df.columns):
                worksheet.write(0, col_idx, col_name)
            for row_idx, row in enumerate(df.rows(), start=1):
                for col_idx, value in enumerate(row):
                    worksheet.write(row_idx, col_idx, value)
                    
        for i, df in enumerate(sal_frac):
            worksheet = workbook.add_worksheet(f"sales_{i}")
            for col_idx, col_name in enumerate(df.columns):
                worksheet.write(0, col_idx, col_name)
            for row_idx, row in enumerate(df.rows(), start=1):
                for col_idx, value in enumerate(row):
                    worksheet.write(row_idx, col_idx, value)
                    
    workbook.close()
    stream.seek(0)
    return stream.getvalue()


if st.session_state.sal_pur is not None and not st.session_state.sal_pur.is_empty():
    st.header("Data Preview")
    st.dataframe(st.session_state.sal_pur, use_container_width=True)
    process_btn = st.button("Process Data")
    if process_btn:
        st.session_state.sal_grouped = Invoiceservice.sal_process(
            st.session_state.sal_pur,
            st.session_state.df_customer,
            start_index=setting.sales_index,
        )
        st.session_state.pur_grouped = Invoiceservice.pur_process(
            st.session_state.sal_pur,
            st.session_state.df_supplier,
            start_index=setting.purchase_index,
        )
        st.session_state.sal_unprocessed = Invoiceservice.sal_unprocess(
            st.session_state.sal_pur,
            st.session_state.df_customer,
        )
        st.session_state.pur_unprocessed = Invoiceservice.pur_unprocess(
            st.session_state.sal_pur,
            st.session_state.df_supplier,
        )
        purchase_invoice, sales_invoice, unprocessed_sales, unprocessed_purchase = st.tabs(
            ["Purchase Invoice", "Sales Invoice", "Unprocessed Sales", "Unprocessed Purchase"]
        )
        
        with purchase_invoice:
            st.header("Purchase Invoice")
            if st.session_state.pur_grouped is not None and not st.session_state.pur_grouped.is_empty():
                st.dataframe(st.session_state.pur_grouped, use_container_width=True)
            else:
                st.write("No data found.")
                
        with sales_invoice:
            st.header("Sales Invoice")
            if st.session_state.sal_grouped is not None and not st.session_state.sal_grouped.is_empty():
                st.dataframe(st.session_state.sal_grouped, use_container_width=True)
            else:
                st.write("No data found.")
                
        with unprocessed_purchase:
            st.header("Unprocessed Purchase")
            if st.session_state.pur_unprocessed is not None and not st.session_state.pur_unprocessed.is_empty():
                st.dataframe(st.session_state.pur_unprocessed, use_container_width=True)
            else:
                st.write("No data found.")
                
        with unprocessed_sales:
            st.header("Unprocessed Sales")
            if st.session_state.sal_unprocessed is not None and not st.session_state.sal_unprocessed.is_empty():
                st.dataframe(st.session_state.sal_unprocessed, use_container_width=True)
            else:
                st.write("No data found.")
        
        download_btn = st.download_button(
            label="Download Excel",
            data=export_excel(),
            file_name="purchase_sales_invoice.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )



# endregion

