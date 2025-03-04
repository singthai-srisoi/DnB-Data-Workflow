import polars as pl
from typing import List
import streamlit as st
import fastexcel as fex
import io


st.title("Purchase Invoice Data Migration")
st.header("Upload File")
data_file = st.file_uploader("Upload a file", type=["xlsx"])

sheet_name = None
if data_file:
    # data_file to bytes
    data_file_ = data_file.read()
    sheets = fex.read_excel(data_file_).sheet_names
    st.text("select sheet name")
    sheet_name = st.selectbox("Sheet Name", sheets)

st.header("Start Index")
st.info("Start index will be the starting invoice number")
start_index = st.number_input("Start Index", value=2)

st.header("Output File Name")
export_file_name = st.text_input("Output File Name", "purchase_invoice")

customer = pl.read_excel(
    "./data/excel data/Maintain Customer.xlsx",
    sheet_name="Maintain Customer",
)
supplier = pl.read_excel(
    "./data/excel data/Maintain Supplier.xlsx",
    sheet_name="Maintain Supplier",
)

def process(file, name):
    sal_pur = pl.read_excel(
        # "./data/excel data/bst jan2025 sales&pur ffb.xlsx",
        # sheet_name="jan2025",
        file,
        sheet_name=name,
    )

    sal_pur = sal_pur.filter(
        (sal_pur["Date Out"].is_not_null()) & (sal_pur["Supplier"].is_not_null())
    )

    sal_pur = sal_pur.join(supplier, left_on="Supplier", right_on="Code", how="inner")
    sal_pur = sal_pur.select([
        "Date Out",
        "Supplier",
        "Net Wt(Ton)",
        "Price(ton)",
    ]).rename({
        "Date Out": "DocDate",
        "Supplier": "Code",
        "Net Wt(Ton)": "Qty",
        "Price(ton)": "U/Price",
    })

    sal_pur_grouped = (
        sal_pur
        .group_by(
            ["Code"],
            maintain_order=True
        )
        .all()
        .with_row_index(offset=start_index)
        .with_columns(
            Seq = pl.col("DocDate").list.len().map_elements(lambda x: list(range(1, x + 1)), return_dtype=List[int]),
            # DocNo = f"PI-{pl.col('index'):0>5}"
            DocNo = pl.col("index").map_elements(lambda x: f"PI-{x:0>5}")
        )
        .drop(["index"])
        .explode("DocDate", "Seq", "Qty", "U/Price")
    )
    return sal_pur_grouped

def export(data: pl.DataFrame):
    stream = io.BytesIO()
    data.write_excel(stream)
    stream.seek(0)
    return stream.read()


if data_file and sheet_name and start_index:
    t = st.text("Processing...")
    sal_pur_grouped = process(data_file, sheet_name)
    if not sal_pur_grouped.is_empty():
        
        t.empty()
        st.header("Preview")
        st.dataframe(sal_pur_grouped)
        st.download_button(
            label="Download Report",
            data=export(sal_pur_grouped),
            file_name=f"{export_file_name}.xlsx",
            mime="text/csv",
        )
    else:
        st.error("Error in processing data")
    

# record of supplier that is not in the supplier table

# sal_pur_ = pl.read_excel(
#     "./data/excel data/bst jan2025 sales&pur ffb.xlsx",
#     sheet_name="jan2025",
# )
# supplier_ = pl.read_excel(
#     "./data/excel data/Maintain Supplier.xlsx",
#     sheet_name="Maintain Supplier",
# )
# supplier_ = supplier_.with_columns(
#     Supplier_Code = pl.col("Code")
# )
# sal_pur_.join(supplier_, left_on="Supplier", right_on="Code", how="left").filter(pl.col("Supplier_Code").is_null())




