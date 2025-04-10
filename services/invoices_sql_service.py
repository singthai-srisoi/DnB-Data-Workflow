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
            supplier = pl.col("supplier").cast(pl.String).map_elements(lambda x: f"{x.replace(' ', '')}", return_dtype=pl.String),
        )
        
        if supplier_data is not None and not supplier_data.is_empty():
            pur = pur.join(supplier_data, left_on="supplier", right_on="code", how="inner")

        pur = pur.select(
            pl.col("date_out").cast(pl.Date, strict=False),
            pl.col("supplier").cast(pl.Utf8, strict=False),
            pl.col("net_wt_ton").cast(pl.Float64, strict=False),
            pl.col("price_ton").cast(pl.Float64, strict=False),
            pl.col("gross_amt").cast(pl.Float64, strict=False),
            pl.col("serial_no").cast(pl.Utf8, strict=False),
        ).rename({
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
            .with_columns(
                pl.col("DocDate").dt.strftime("%d/%m/%Y"),
                UOM = pl.lit("TON"),
            )
        )
        return pur_grouped
    
    # ! Process Sales Data
    @classmethod
    def sal_process(cls, data: pl.DataFrame, customer_data: pl.DataFrame = None, start_index: int = 1) -> pl.DataFrame:
        sal = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").cast(pl.String).map_elements(lambda x: f"300-{x.replace(' ', '')}", return_dtype=pl.String),
        )
        
        if customer_data is not None and not customer_data.is_empty():
            sal = sal.join(customer_data, left_on="supplier", right_on="code", how="inner")
            
        tpt = sal.select(
        #     [
        #     "date_out",
        #     "supplier",
        #     "net_wt_ton",
        #     "tpt_chrg",
        #     "tpt_amt",
        #     "serial_no",
        # ]
            pl.col("date_out").cast(pl.Date, strict=False),
            pl.col("supplier").cast(pl.Utf8, strict=False),
            pl.col("net_wt_ton").cast(pl.Float64, strict=False),
            pl.col("tpt_chrg").cast(pl.Float64, strict=False),
            pl.col("tpt_amt").cast(pl.Float64, strict=False),
            pl.col("serial_no").cast(pl.Utf8, strict=False),
        ).rename({
            "date_out": "DocDate",
            "supplier": "Code",
            "net_wt_ton": "Qty",
            "tpt_chrg": "UnitPrice",
            "tpt_amt": "Amount",
            "serial_no": "Remark1",
        }).drop_nulls(["UnitPrice", "Amount"]).with_columns(
            ItemCode = pl.lit("500-002"),
            Account = pl.lit("500-002"),
            UOM = pl.lit("TON"),
        )

        worker = sal.select(
        # [
        #     "date_out",
        #     "supplier",
        #     "net_wt_ton",
        #     "worker_chrg",
        #     "worker_amt",
        #     "serial_no",
        # ]
            pl.col("date_out").cast(pl.Date, strict=False),
            pl.col("supplier").cast(pl.Utf8, strict=False),
            pl.col("net_wt_ton").cast(pl.Float64, strict=False),
            pl.col("worker_chrg").cast(pl.Float64, strict=False),
            pl.col("worker_amt").cast(pl.Float64, strict=False),
            pl.col("serial_no").cast(pl.Utf8, strict=False),
        ).rename({
            "date_out": "DocDate",
            "supplier": "Code",
            "net_wt_ton": "Qty",
            "worker_chrg": "UnitPrice",
            "worker_amt": "Amount",
            "serial_no": "Remark1",
        }).drop_nulls(["UnitPrice", "Amount"]).with_columns(
            ItemCode = pl.lit("500-003"),
            Account = pl.lit("500-003"),
            UOM = pl.lit("UNIT"),
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
            .explode("DocDate", "Seq", "Qty", "UnitPrice", "Amount", "ItemCode", "Account", "Remark1", "UOM")
            .with_columns(
                pl.col("DocDate").dt.strftime("%d/%m/%Y"),
            )
        )

        return combined
    
    @classmethod
    def pur_unprocess(cls, data: pl.DataFrame, supplier_data: pl.DataFrame) -> pl.DataFrame:
        pur = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").cast(pl.String).map_elements(lambda x: f"{x.replace(' ', '')}", return_dtype=pl.String),
        )
        # check by joinning with supplier data, if not found, it will be in unprocessed data
        pur = pur.join(supplier_data, left_on="supplier", right_on="code", how="anti")
        return pur

    @classmethod
    def sal_unprocess(cls, data: pl.DataFrame, customer_data: pl.DataFrame) -> pl.DataFrame:
        sal = data.filter(
            (data["date_out"].is_not_null()) & (data["supplier"].is_not_null())
        ).with_columns(
            supplier = pl.col("supplier").cast(pl.String).map_elements(lambda x: f"300-{x.replace(' ', '')}", return_dtype=pl.String),
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
    
from pydantic import BaseModel
import datetime
from win32com.client import CDispatch

class SL_IV_Detail(BaseModel):
    Seq: int
    Account: str
    Remark1: str
    ItemCode: str
    Qty: float
    UnitPrice: float
    Amount: float = 0.0
    
    def set_field(self, lDetail):
        lDetail.Append()
        lDetail.FindField("Seq").value = self.Seq
        lDetail.FindField("Account").AsString = self.Account
        lDetail.FindField("ItemCode").AsString = self.ItemCode
        lDetail.FindField("Qty").AsFloat = self.Qty
        lDetail.FindField("UnitPrice").AsFloat = self.UnitPrice
        lDetail.FindField("Amount").AsFloat = self.Amount
        lDetail.FindField("Remark1").AsString = self.Remark1
        lDetail.Post()
        
class SL_IV(BaseModel):
    DocNo: str
    DocDate: str
    Code: str
    Description: str = ""
    
    cdsDocDetail: List[SL_IV_Detail] = []
    
    def set_field(self, lMain, post_date: str = datetime.datetime.now().strftime('%d/%m/%Y')):
        lMain.FindField("DocNo").AsString = self.DocNo
        lMain.FindField("DocDate").value = self.DocDate
        lMain.FindField("PostDate").value = post_date
        lMain.FindField("Code").AsString = self.Code
        lMain.FindField("Description").AsString = self.Description
        
    def post(self, ComServer: CDispatch):
        BizObject = ComServer.BizObjects.Find("SL_IV")
        lMain = BizObject.DataSets.Find("MainDataSet")
        lDetail = BizObject.DataSets.Find("cdsDocDetail")
        
        lDocKey = BizObject.FindKeyByRef("DocNo", self.DocNo)
        
        if lDocKey is None:
            BizObject.New()
            self.set_field(lMain)
            
            for detail in self.cdsDocDetail:
                detail.set_field(lDetail)
        else:
            raise Exception(f"Record Found, cannot create new record")
        
        try:
            BizObject.Save()
            print(f"Posting {self.DocNo} Done")
        except Exception as e:
            print("Oops!", e)
        BizObject.Close()
        
class PH_PI_Detail(SL_IV_Detail):
    def set_field(self, lDetail):
        lDetail.Append()
        lDetail.FindField("Seq").value = self.Seq
        lDetail.FindField("Account").AsString = self.Account
        lDetail.FindField("ItemCode").AsString = self.ItemCode
        lDetail.FindField("Qty").AsFloat = self.Qty
        lDetail.FindField("UnitPrice").AsFloat = self.UnitPrice
        lDetail.FindField("Amount").AsFloat = self.Amount
        lDetail.FindField("Remark1").AsString = self.Remark1
        lDetail.Post()

class PH_PI(SL_IV):
    def set_field(self, lMain, post_date: str = datetime.datetime.now().strftime('%d/%m/%Y')):
        lMain.FindField("DocNo").AsString = self.DocNo
        lMain.FindField("DocDate").value = self.DocDate
        lMain.FindField("PostDate").value = post_date
        lMain.FindField("Code").AsString = self.Code
        lMain.FindField("Description").AsString = self.Description
        
    def post(self, ComServer: CDispatch):
        BizObject = ComServer.BizObjects.Find("PH_PI")
        lMain = BizObject.DataSets.Find("MainDataSet")
        lDetail = BizObject.DataSets.Find("cdsDocDetail")
        
        lDocKey = BizObject.FindKeyByRef("DocNo", self.DocNo)
        
        if lDocKey is None:
            BizObject.New()
            self.set_field(lMain)
            
            for detail in self.cdsDocDetail:
                detail.set_field(lDetail)
        else:
            raise Exception(f"Record Found, cannot create new record")
        
        try:
            BizObject.Save()
            print(f"Posting {self.DocNo} Done")
        except Exception as e:
            print("Oops!", e)
        BizObject.Close()
