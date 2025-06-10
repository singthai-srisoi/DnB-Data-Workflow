import streamlit as st
import polars as pl
import fastexcel as fex
import xlsxwriter
from typing import Callable
import io

from database import get_db_session
from models import Supplier, Customer
from models.settings import Setting
from services.invoices_sql_service import *
from sqlacc import get_comserver


# ---------------------------
# Utility Functions
# ---------------------------

def load_customers_and_suppliers():
    session = next(get_db_session())
    customers = session.query(Customer).all()
    suppliers = session.query(Supplier).all()
    setting: Setting = session.query(Setting).first()

    df_customer = pl.DataFrame([c.__dict__ for c in customers]).drop(['_sa_instance_state'])
    df_supplier = pl.DataFrame([s.__dict__ for s in suppliers]).drop(['_sa_instance_state'])

    return df_customer, df_supplier, setting, session


def export_excel():
    pur_frac = Invoiceservice.fraction_df(st.session_state.pur_grouped, "Code")
    sal_frac = Invoiceservice.fraction_df(st.session_state.sal_grouped, "Code")
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
    stream.seek(0)
    return stream.getvalue()


def set_index_max():
    setting = st.session_state.setting
    session = st.session_state.session

    pur_docs = st.session_state.pur_grouped["DocNo"].str.extract(r"(\d+)").cast(pl.Int32)
    if not pur_docs.is_null().all():
        max_pur = pur_docs.max()
        if max_pur is not None:
            setting.purchase_index = max_pur + 1

    sal_docs = st.session_state.sal_grouped["DocNo"].str.extract(r"(\d+)").cast(pl.Int32)
    if not sal_docs.is_null().all():
        max_sal = sal_docs.max()
        if max_sal is not None:
            setting.sales_index = max_sal + 1

    session.flush()
    session.commit()
    session.refresh(setting)


def post_sqlacc(progress=None, log: Callable[[str], None] = None):
    try:
        ComServer = get_comserver()
        st.text("Posting Purchase Invoice to SQLAcc")

        pur_grouped: pl.DataFrame = st.session_state.pur_grouped
        sal_grouped: pl.DataFrame = st.session_state.sal_grouped
        total_docs = pur_grouped["DocNo"].n_unique() + sal_grouped["DocNo"].n_unique()

        for i, docno in enumerate(pur_grouped["DocNo"].unique().sort().to_list()):
            group = pur_grouped.filter(pl.col("DocNo") == docno)
            invoice = PH_PI(
                DocNo=docno,
                DocDate=group["DocDate"].first(),
                Code=group["Code"].first(),
            )
            invoice.cdsDocDetail = [
                PH_PI_Detail(**row) for row in group.iter_rows(named=True)
            ]
            try:
                invoice.post(ComServer)
                log(f"Posted {docno} to SQLAcc") if log else st.text(f"Posted {docno} to SQLAcc")
                if progress:
                    progress.progress((i + 1) / total_docs, text=f"Posting {docno} to SQLAcc")
            except Exception as e:
                st.text(f"Error posting {docno}: {e}")

        st.text("Posting Sales Invoice to SQLAcc")
        for i, docno in enumerate(sal_grouped["DocNo"].unique().sort().to_list()):
            group = sal_grouped.filter(pl.col("DocNo") == docno)
            invoice = SL_IV(
                DocNo=docno,
                DocDate=group["DocDate"].first(),
                Code=group["Code"].first(),
            )
            invoice.cdsDocDetail = [
                SL_IV_Detail(**row) for row in group.iter_rows(named=True)
            ]
            try:
                invoice.post(ComServer)
                log(f"Posted {docno} to SQLAcc") if log else st.text(f"Posted {docno} to SQLAcc")
                if progress:
                    progress.progress((i + 1) / total_docs, text=f"Posting {docno} to SQLAcc")
            except Exception as e:
                st.text(f"Error posting {docno}: {e}")
    except Exception as e:
        st.text(f"Error initializing SQLAcc: {e}")


# ---------------------------
# Streamlit App Main
# ---------------------------

# def main():
st.title("Purchase Invoice Data Migration")
st.header("Upload File")

if "df_customer" not in st.session_state or "df_supplier" not in st.session_state or "setting" not in st.session_state:
    df_customer, df_supplier, setting, session = load_customers_and_suppliers()
    st.session_state.df_customer = df_customer
    st.session_state.df_supplier = df_supplier
    st.session_state.setting = setting
    st.session_state.session = session

data_file = st.file_uploader("Upload a file", type=["xlsx"])
if data_file:
    data_file_ = data_file.read()
    sheets = fex.read_excel(data_file_).sheet_names
    sheet_name = st.selectbox("Select Sheet", sheets)
    if sheet_name:
        st.session_state.sal_pur = pl.read_excel(data_file_, sheet_name=sheet_name)
        st.session_state.sal_pur = st.session_state.sal_pur.rename({
            col: Invoiceservice.clean_column(col) for col in st.session_state.sal_pur.columns
        })

if st.session_state.get("sal_pur") is not None and not st.session_state.sal_pur.is_empty():
    st.header("Data Preview")
    st.dataframe(st.session_state.sal_pur, use_container_width=True)

    if st.button("Process Data"):
        df_customer, df_supplier, setting, session = load_customers_and_suppliers()
        st.session_state.df_customer = df_customer
        st.session_state.df_supplier = df_supplier
        st.session_state.setting = setting
        st.session_state.session = session

        st.session_state.sal_grouped = Invoiceservice.sal_process(st.session_state.sal_pur, df_customer, setting.sales_index)
        st.session_state.pur_grouped = Invoiceservice.pur_process(st.session_state.sal_pur, df_supplier, setting.purchase_index)
        st.session_state.sal_unprocessed = Invoiceservice.sal_unprocess(st.session_state.sal_pur, df_customer)
        st.session_state.pur_unprocessed = Invoiceservice.pur_unprocess(st.session_state.sal_pur, df_supplier)

if st.session_state.get("sal_grouped") is not None and st.session_state.get("pur_grouped") is not None:
    tabs = st.tabs(["Purchase Invoice", "Sales Invoice", "Unprocessed Purchase", "Unprocessed Sales"])
    setting = st.session_state.setting
    session = st.session_state.session

    with tabs[0]:
        st.header("Purchase Invoice")
        pur_index = st.number_input("Start Index", min_value=1, value=setting.purchase_index, step=1, key="pur_index")
        if pur_index:
            setting.purchase_index = pur_index
            session.flush(); session.commit(); session.refresh(setting)
            st.session_state.pur_grouped = Invoiceservice.pur_process(
                st.session_state.sal_pur,
                st.session_state.df_supplier,
                start_index=setting.purchase_index,
            )
        st.dataframe(st.session_state.pur_grouped, use_container_width=True)

    with tabs[1]:
        st.header("Sales Invoice")
        sal_index = st.number_input("Start Index", min_value=1, value=setting.sales_index, step=1, key="sal_index")
        if sal_index:
            setting.sales_index = sal_index
            session.flush(); session.commit(); session.refresh(setting)
            st.session_state.sal_grouped = Invoiceservice.sal_process(
                st.session_state.sal_pur,
                st.session_state.df_customer,
                start_index=setting.sales_index,
            )
        st.dataframe(st.session_state.sal_grouped, use_container_width=True)

    with tabs[2]:
        st.header("Unprocessed Purchase")
        st.dataframe(st.session_state.pur_unprocessed, use_container_width=True)

    with tabs[3]:
        st.header("Unprocessed Sales")
        st.dataframe(st.session_state.sal_unprocessed, use_container_width=True)

    st.download_button(
        label="Download Excel",
        data=export_excel(),
        file_name="purchase_sales_invoice.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        on_click=set_index_max
    )

    if st.button("Post to SQLAcc", help="Post to SQLAcc. Note: This is only a testing feature!!!!"):
        progress = st.progress(0, text="Posting to SQLAcc...")
        log_box = st.empty()
        log_lines = []

        def log(msg: str):
            log_lines.append(msg)
            log_box.code("\n".join(log_lines), language="text", height=200)

        post_sqlacc(progress=progress, log=log)
        set_index_max()
        st.success("Posted to SQLAcc!")


# ---------------------------
# Entry Point
# ---------------------------
if __name__ == "__main__":
    main()
