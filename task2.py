import polars as pl
import pathlib
import os
import streamlit as st

data: pl.DataFrame | None = None

def process_data(
    data_path: str,
    data_sep: str,
    supplier_map_path: str = "data/Supplier Name Map.csv",
    supplier_map_sep: str = "\t",
    customer_map_path: str = "data/Customer Name Map.csv",
    customer_map_sep: str = ",",
    output_path: str = "data/export",
    export_name: str = "jan2025_abb.csv",
):
    data = (
        pl.read_csv(data_path, separator=data_sep)
        .with_columns(
            pl.col("date").cast(pl.Date),
        )
    )

    # contain the supplier and customer name mapping
    supplier_abb = pl.read_csv(supplier_map_path, separator=supplier_map_sep).rename({'abb': 'supplier_abb'})
    # customer_abb = pl.read_csv("data/Customer Name Map.csv", separator=',').rename({'abb': 'customer_abb'})
    customer_abb = pl.read_csv(customer_map_path, separator=customer_map_sep).rename({'abb': 'customer_abb'})

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

    agg_data = (
        data
        .group_by("supplier", "customer", "product", maintain_order=True)
        .agg(
            pl.col("factory_nett").sum().alias("factory_nett"),
            pl.col("deduction").sum().alias("deduction"),
        )
        .filter(pl.col("product") == "001 - FFB")
    )

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

    dirpath = pathlib.Path(output_path)
    if not dirpath.exists():
        dirpath.mkdir(parents=True)
    pivot_data.write_csv(dirpath / export_name)

def to_csv(data: pl.DataFrame):
    return data.write_csv()

# Streamlit UI
st.title("CSV Processing App")

# File Upload Inputs
st.header("Upload Files")
data_file = st.file_uploader("Upload Data File", type=["csv"])

# Delimiter Selection
st.header("Select Delimiters")
data_sep = st.selectbox("Delimiter for Data File", [",", ";", "\t", "|"], index=0)

# export file name
output_path = st.text_input("Output File Name", "report.csv")

# Process and Export Button
if st.button("Process & Export"):
    if data_file:
        pivot_data = process_data(
            data_path=data_file,
            data_sep=data_sep,
            output_path="data/export",
            export_name=output_path,
        )

        # Save to Downloads Folder
        download_path = os.path.join(os.path.expanduser("~"), "Downloads", "jan2025_abb.csv")
        pivot_data.write_csv(download_path)

        st.success(f"File successfully exported to {download_path}")
    else:
        st.error("Please upload all required files.")

