from typing import List
import re
import polars as pl


class Invoiceservice:
    @classmethod    
    def clean_column(cls, name):
        name = name.lower()
        name = re.sub(r'\W+', ' ', name)
        name = name.strip()
        name = name.replace(' ', '_')
        return name
    
    # ! Process Purchase Data
    @classmethod
    def pur_process(cls, data: pl.DataFrame, supplier_data: pl.DataFrame = None, start_index: int = 1) -> pl.DataFrame:
        pur = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").map_elements(lambda x: f"{x.replace(' ', '')}", return_dtype=pl.String),
        )
        
        if supplier_data is not None and not supplier_data.is_empty():
            pur = pur.join(supplier_data, left_on="supplier", right_on="code", how="inner")

        pur = pur.select([
            "date_out",
            "supplier",
            "net_wt_ton",
            "price_ton",
            "gross_amt",
            "serial_no",
        ]).rename({
            "date_out": "DocDate",
            "supplier": "Code",
            "net_wt_ton": "Qty",
            "price_ton": "UnitPrice",
            "gross_amt": "Amount",
            "serial_no": "Remark1",
        })

        pur_grouped = (
            pur
            .group_by(
                ["Code"],
                maintain_order=True
            )
            .all()
            .with_row_index(offset=start_index)
            .with_columns(
                Seq = pl.col("DocDate").list.len().map_elements(lambda x: list(range(1, x + 1)), return_dtype=pl.List(pl.Int32)),
                DocNo = pl.col("index").map_elements(lambda x: f"PI-{x:0>5}", return_dtype=pl.String),
                ItemCode = pl.lit("610-001"),
                Account = pl.lit("610-000"),
            )
            .drop(["index"])
            .explode("DocDate", "Seq", "Qty", "UnitPrice", "Remark1", "Amount")
        )
        return pur_grouped
    
    # ! Process Sales Data
    @classmethod
    def sal_process(cls, data: pl.DataFrame, customer_data: pl.DataFrame = None, start_index: int = 1) -> pl.DataFrame:
        sal = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").map_elements(lambda x: f"300-{x.replace(' ', '')}", return_dtype=pl.String),
        )
        
        if customer_data is not None and not customer_data.is_empty():
            sal = sal.join(customer_data, left_on="supplier", right_on="code", how="inner")
            
        tpt = sal.select([
            "date_out",
            "supplier",
            "net_wt_ton",
            "tpt_chrg",
            "tpt_amt",
            "serial_no",
        ]).rename({
            "date_out": "DocDate",
            "supplier": "Code",
            "net_wt_ton": "Qty",
            "tpt_chrg": "UnitPrice",
            "tpt_amt": "Amount",
            "serial_no": "Remark1",
        }).drop_nulls(["UnitPrice", "Amount"]).with_columns(
            ItemCode = pl.lit("500-002"),
            Account = pl.lit("500-000"),
        )

        worker = sal.select([
            "date_out",
            "supplier",
            "net_wt_ton",
            "worker_chrg",
            "worker_amt",
            "serial_no",
        ]).rename({
            "date_out": "DocDate",
            "supplier": "Code",
            "net_wt_ton": "Qty",
            "worker_chrg": "UnitPrice",
            "worker_amt": "Amount",
            "serial_no": "Remark1",
        }).drop_nulls(["UnitPrice", "Amount"]).with_columns(
            ItemCode = pl.lit("500-003"),
            Account = pl.lit("500-000"),
        )

        combined =(tpt.vstack(worker)
            .group_by(
                ["Code"],
                maintain_order=True
            )
            .all()
            .with_row_index(offset=start_index)
            .with_columns(
                Seq = pl.col("DocDate").list.len().map_elements(lambda x: list(range(1, x + 1)), return_dtype=pl.List(pl.Int32)),
                # DocNo = f"PI-{pl.col('index'):0>5}"
                DocNo = pl.col("index").map_elements(lambda x: f"IV-{x:0>5}", return_dtype=pl.String),
                # ItemCode = "610-001",
                # Account = "610-000"
            )
            .drop(["index"])
            .explode("DocDate", "Seq", "Qty", "UnitPrice", "Amount", "ItemCode", "Account", "Remark1")
        )

        return combined
    
    @classmethod
    def pur_unprocess(cls, data: pl.DataFrame, supplier_data: pl.DataFrame) -> pl.DataFrame:
        pur = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").map_elements(lambda x: f"{x.replace(' ', '')}", return_dtype=pl.String),
        )
        # check by joinning with supplier data, if not found, it will be in unprocessed data
        pur = pur.join(supplier_data, left_on="supplier", right_on="code", how="anti")
        return pur

    @classmethod
    def sal_unprocess(cls, data: pl.DataFrame, customer_data: pl.DataFrame) -> pl.DataFrame:
        sal = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").map_elements(lambda x: f"300-{x.replace(' ', '')}", return_dtype=pl.String),
        )
        # check by joinning with customer data, if not found, it will be in unprocessed data
        sal = sal.join(customer_data, left_on="supplier", right_on="code", how="anti")
        return sal
    
    @classmethod
    def fraction_df(cls, data: pl.DataFrame, key: str = "Code") -> List[pl.DataFrame]:
        if data.is_empty():
            return []
        # Add a row index to help us track positions
        data = data.with_row_index(name="row_idx")
        # Group by 'Code' and get list of rows for each group
        groups = data.group_by(key, maintain_order=True).agg(
            pl.col("row_idx")
        )
        chunks = []
        current_chunk = []
        current_count = 0
        i = 0
        while i < len(groups):
            group_indices = groups[i, "row_idx"]
            group_size = len(group_indices)

            # If adding this group exceeds 100 rows, flush current chunk
            if current_count + group_size > 100:
                if current_chunk:
                    chunk_df = data.filter(pl.col("row_idx").is_in(current_chunk)).drop("row_idx")
                    chunks.append(chunk_df)
                current_chunk = []
                current_count = 0

            # Add group to current chunk
            current_chunk.extend(group_indices)
            current_count += group_size
            i += 1

        # Add last chunk if not empty
        if current_chunk:
            chunk_df = data.filter(pl.col("row_idx").is_in(current_chunk)).drop("row_idx")
            chunks.append(chunk_df)

        return chunks