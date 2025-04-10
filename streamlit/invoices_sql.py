import streamlit as st
import polars as pl
import fastexcel as fex
import xlsxwriter

from database import get_db_session
from models import Supplier, Customer
from models.settings import Setting
from services.invoices_sql_service import *
import io

# region: Preload Page
# get supplier and customer data from database
session = next(get_db_session())
customers = session.query(Customer).all()
customers = [row.__dict__ for row in customers]
suppliers = session.query(Supplier).all()
suppliers = [row.__dict__ for row in suppliers]
setting: Setting = session.query(Setting).first()

# console log setting
print(f"Setting: {setting.__dict__}")

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

def set_index_max():
    max_pur = st.session_state.pur_grouped["DocNo"].str.extract(r"(\d+)").cast(pl.Int32).max()
    max_sal = st.session_state.sal_grouped["DocNo"].str.extract(r"(\d+)").cast(pl.Int32).max()
    setting.purchase_index = max_pur + 1
    setting.sales_index = max_sal + 1
    session.flush()
    session.commit()
    session.refresh(setting)


def post_sqlacc():
    import win32com.client
    import pythoncom
    
    pythoncom.CoInitialize()  # Initialize COM in this thread
    
    try:
        ComServer = win32com.client.Dispatch("SQLAcc.BizApp")
        
        
        # reigion: Post Purchase Invoice
        st.text("Posting Purchase Invoice to SQLAcc")
        pur_grouped: pl.DataFrame = st.session_state.pur_grouped
        unique_docno = pur_grouped["DocNo"].unique()
        
        
        # iterate through each docno and post to sqlacc
        for docno in unique_docno:
            group = pur_grouped.filter(pl.col("DocNo") == docno)
            invoice = SL_IV(
                DocNo = docno,
                DocDate = group["DocDate"].first(),
                Code = group["Code"].first(),
            )
            details = []
            for row in group.iter_rows(named=True):
                detail = SL_IV_Detail(
                    Seq = row["Seq"],
                    Account = row["Account"],
                    Remark1 = row["Remark1"],
                    ItemCode = row["ItemCode"],
                    Qty = row["Qty"],
                    UnitPrice = row["UnitPrice"],
                    Amount = row["Amount"],                
                )
                print(detail.model_dump())
                details.append(detail)
            invoice.cdsDocDetail = details
            try:
                invoice.post(ComServer)
                st.text(f"Posted {docno} to SQLAcc")
            except Exception as e:
                st.text(f"Error posting {docno} to SQLAcc: {e}")
        # endregion
        

        # region: Post Sales Invoice
        st.text("Posting Sales Invoice to SQLAcc")
        sal_grouped: pl.DataFrame = st.session_state.sal_grouped
        unique_docno = sal_grouped["DocNo"].unique()
        
        # iterate through each docno and post to sqlacc
        for docno in unique_docno:
            group = sal_grouped.filter(pl.col("DocNo") == docno)
            invoice = SL_IV(
                DocNo = docno,
                DocDate = group["DocDate"].first(),
                Code = group["Code"].first(),
            )
                
            details = []
            for row in group.iter_rows(named=True):
                detail = SL_IV_Detail(
                    Seq = row["Seq"],
                    Account = row["Account"],
                    Remark1 = row["Remark1"],
                    ItemCode = row["ItemCode"],
                    Qty = row["Qty"],
                    UnitPrice = row["UnitPrice"],
                    Amount = row["Amount"],                
                )
                print(detail.model_dump())
                details.append(detail)
            invoice.cdsDocDetail = details
            try:
                invoice.post(ComServer)
                st.text(f"Posted {docno} to SQLAcc")
            except Exception as e:
                st.text(f"Error posting {docno} to SQLAcc: {e}")
            
        # endregion
    except Exception as e:
        st.text(f"Error initializing SQLAcc: {e}")

if st.session_state.sal_pur is not None and not st.session_state.sal_pur.is_empty():
    st.header("Data Preview")
    st.dataframe(st.session_state.sal_pur, use_container_width=True, key="sal_pur")
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

if (st.session_state.sal_grouped is not None and st.session_state.pur_grouped is not None):
    purchase_invoice, sales_invoice, unprocessed_sales, unprocessed_purchase = st.tabs(
        ["Purchase Invoice", "Sales Invoice", "Unprocessed Sales", "Unprocessed Purchase"]
    )
    
    with purchase_invoice:
        st.header("Purchase Invoice")
        # st.text(f"purchase index => {setting.purchase_index}")
        pur_index = st.number_input("Start Index", min_value=1, value=setting.purchase_index, step=1, key="pur_index")
        if pur_index:
            setting.purchase_index = pur_index
            session.flush()
            session.commit()
            session.refresh(setting)
            st.session_state.pur_grouped = Invoiceservice.pur_process(
                st.session_state.sal_pur,
                st.session_state.df_supplier,
                start_index=setting.purchase_index,
            )
        if st.session_state.pur_grouped is not None and not st.session_state.pur_grouped.is_empty():
            st.dataframe(st.session_state.pur_grouped, use_container_width=True, key="pur_grouped")
        else:
            st.write("No data found.")
            
    with sales_invoice:
        st.header("Sales Invoice")
        sal_index = st.number_input("Start Index", min_value=1, value=setting.sales_index, step=1, key="sal_index")
        if sal_index:
            setting.sales_index = sal_index
            session.flush()
            session.commit()
            session.refresh(setting)
            st.session_state.sal_grouped = Invoiceservice.sal_process(
                st.session_state.sal_pur,
                st.session_state.df_customer,
                start_index=setting.sales_index,
            )
        if st.session_state.sal_grouped is not None and not st.session_state.sal_grouped.is_empty():
            st.dataframe(st.session_state.sal_grouped, use_container_width=True, key="sal_grouped")
        else:
            st.write("No data found.")
            
    with unprocessed_purchase:
        st.header("Unprocessed Purchase")
        if st.session_state.pur_unprocessed is not None and not st.session_state.pur_unprocessed.is_empty():
            st.dataframe(st.session_state.pur_unprocessed, use_container_width=True, key="pur_unprocessed")
        else:
            st.write("No data found.")
            
    with unprocessed_sales:
        st.header("Unprocessed Sales")
        if st.session_state.sal_unprocessed is not None and not st.session_state.sal_unprocessed.is_empty():
            st.dataframe(st.session_state.sal_unprocessed, use_container_width=True, key="sal_unprocessed")
        else:
            st.write("No data found.")
    
    download_btn = st.download_button(
        label="Download Excel",
        data=export_excel(),
        file_name="purchase_sales_invoice.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_excel",
    )
    
    post_btn = st.button("Post to SQLAcc", key="post_sqlacc", help="Post to SQLAcc. Note: This is only a tetsing feature!!!!")
    
    if download_btn:
        set_index_max()
        st.success("Download completed!")
        
    if post_btn:
        post_sqlacc()
        set_index_max()
        st.success("Posted to SQLAcc!")



# endregion

