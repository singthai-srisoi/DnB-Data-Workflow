import polars as pl
import pathlib
import os
import streamlit as st

st.title("Report")
st.header("Upload File")
data_file = st.file_uploader("Upload a file", type=["csv"])
st.header("Select Delimiters")
data_sep = st.selectbox("Delimiter for Data File", [",", ";", "\t", "|"], index=0)
st.header("Export File Name")
export_name = st.text_input("Output File Name", "report")


def process(data_file, data_sep):
    data = (
        # "data/DnB/report (2).csv")
        pl.read_csv(data_file, separator=data_sep)
        .with_columns(
            pl.col("date").cast(pl.Date),
        )
    )
    # print('<<< data read! >>>')
    # print(data.head())

    # contain the supplier and customer name mapping
    supplier_abb = pl.read_csv("data/Supplier Name Map.csv", separator='\t').rename({'abb': 'supplier_abb'})
    customer_abb = pl.read_csv("data/Customer Name Map.csv", separator=',').rename({'abb': 'customer_abb'})
    # print('<<< supplier_abb and customer_abb read! >>>')

    data = (
        data
        .join(supplier_abb, on="supplier", how="left")
        .join(customer_abb, on="customer", how="left")
    ).rename(
        {
            'supplier': 'supplier_name',
            'customer': 'customer_name',
            'supplier_abb': 'supplier',
            'customer_abb': 'customer',
        }
    )
    # print('<<< data joined! >>>')

    agg_data = (
        data
        .group_by("supplier", "customer", "product", maintain_order=True)
        .agg(
            pl.col("factory_nett").sum().alias("factory_nett"),
            pl.col("deduction").sum().alias("deduction"),
        )
        .filter(pl.col("product") == "001 - FFB")
    )
    # print('<<< data aggregated! >>>')

    pivot_data = agg_data.pivot(
        "customer",
        index="supplier",
        values="factory_nett",
        aggregate_function="first",
        maintain_order=True,
    ).fill_null(0).fill_null(0)

    pivot_data = pivot_data.with_columns(
        TOTAL = pl.sum_horizontal(pivot_data.select(pl.exclude("supplier")))
    )
    # print('<<< pivot_data created! >>>')

    deduction_totals = (
        agg_data
        .group_by("supplier", maintain_order=True)
        .agg(pl.col("deduction").sum().alias("DEDUCTION_TOTAL"))
    )

    pivot_data = (
        pivot_data
        .join(deduction_totals, on="supplier", how="left")
        .with_columns(
            NETT_TOTAL = pl.col("TOTAL") - pl.col("DEDUCTION_TOTAL")
            
        )
    )
    
    # print('<<< deduction_totals joined! >>>')
    # print(pivot_data.head())
    
    return pivot_data


def export(pivot: pl.DataFrame | None):
    if pivot is None:
        return
    return pivot.write_csv()

if data_file:
    t = st.text("Processing...")
    pivot_data = process(data_file, data_sep)
    if not pivot_data.is_empty():
        t.empty()
        st.header("Preview")
        st.dataframe(pivot_data)
        st.download_button(
            label="Download Report",
            data=export(pivot_data),
            file_name=f"{export_name}.csv",
            mime="text/csv",
        )
    else:
        st.error("Error in processing data")
    



