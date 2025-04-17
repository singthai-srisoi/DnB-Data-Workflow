from pydantic import BaseModel
from win32com.client import CDispatch
import datetime
import polars as pl
from typing import List, Dict, Any
from sqlacc import ComServerService, get_comserver
# datetime.datetime.now().strftime('%d/%m/%Y')

class Contra(BaseModel):
    DOCNO : str
    DOCDATE: str
    SL_DOCNO: str
    SL_CODE: str
    SL_DATE: str
    PI_DOCNO: str
    PI_CODE: str
    PI_DATE: str
    PI_AMOUNT: float
    SL_AMOUNT: float
    
    def post(self, ComServer: CDispatch):
        # DocNo = 15
        # DocNo = f"CT-{DocNo:05d}" # DOCNO format
        # # Customer Contra
        AR_CT = ComServer.BizObjects.Find("AR_CT")
        AR_CT_lMain = AR_CT.DataSets.Find("MainDataSet")
        AR_CT_lDetail = AR_CT.DataSets.Find("cdsKnockOff")

        lDocKey = AR_CT.FindKeyByRef("DocNo", self.DOCNO)
        if lDocKey is None:
            AR_CT.New()
            AR_CT_lMain.FindField("DocNo").AsString = self.DOCNO
            AR_CT_lMain.FindField("DocDate").value = self.DOCDATE
            AR_CT_lMain.FindField("PostDate").value = datetime.datetime.now().strftime('%d/%m/%Y')
            AR_CT_lMain.FindField("Code").AsString = self.SL_CODE # "300-1039"
            AR_CT_lMain.FindField("Description").AsString = ""
            AR_CT_lMain.FindField("DocAmt").AsFloat = self.SL_AMOUNT # CONTRA.get('SL_AMOUNT') # 28.00
            
            #Knock Off IV  
            V = ["IV", self.SL_DOCNO]  #DocType, DocNo
            if (AR_CT_lDetail.Locate("DocType;DocNo", V, False, False)) :
                AR_CT_lDetail.Edit()
                AR_CT_lDetail.FindField("KOAmt").AsFloat = self.SL_AMOUNT # CONTRA.get('SL_AMOUNT')
                # CONTRA.get('SL_AMOUNT')
                AR_CT_lDetail.FindField("KnockOff").AsString = "T"
                AR_CT_lDetail.Post()
        else:
            raise Exception(f"Record Found, cannot create new record")

        try:
            AR_CT.Save()
            print(f"Posting Customher Contra {self.DOCNO} Done")
        except Exception as e:
            print("Oops!", e)
            raise e
        AR_CT.Close()

        # # Supplier Contra
        AP_ST = ComServer.BizObjects.Find("AP_ST")
        AP_ST_lMain = AP_ST.DataSets.Find("MainDataSet")
        AP_ST_lDetail = AP_ST.DataSets.Find("cdsKnockOff")

        lDocKey = AP_ST.FindKeyByRef("DocNo", self.DOCNO)
        if lDocKey is None:
            AP_ST.New()
            AP_ST_lMain.FindField("DocNo").AsString = self.DOCNO
            AP_ST_lMain.FindField("DocDate").value = self.DOCDATE
            AP_ST_lMain.FindField("PostDate").value = datetime.datetime.now().strftime('%d/%m/%Y')
            AP_ST_lMain.FindField("Code").AsString = self.PI_CODE # "1039"
            # CONTRA.get('PI_CODE') # "1039"
            AP_ST_lMain.FindField("Description").AsString = ""
            AP_ST_lMain.FindField("DocAmt").AsFloat = self.SL_AMOUNT # CONTRA.get('SL_AMOUNT') # 503.25
            # CONTRA.get('SL_AMOUNT') # 503.25
            
            #Knock Off IV  
            V = ["PI", self.PI_DOCNO]  #DocType, DocNo
            if (AP_ST_lDetail.Locate("DocType;DocNo", V, False, False)) :
                AP_ST_lDetail.Edit()
                AP_ST_lDetail.FindField("KOAmt").AsFloat = self.SL_AMOUNT # CONTRA.get('SL_AMOUNT')
                # CONTRA.get('SL_AMOUNT')
                AP_ST_lDetail.FindField("KnockOff").AsString = "T"
                AP_ST_lDetail.Post()
        else:
            AP_ST.Params.Find("Dockey").Value = lDocKey
            AP_ST.Open()
            AP_ST.Edit()
            AP_ST_lMain.Edit()
            AP_ST_lMain.FindField("Code").AsString = self.PI_CODE
            AP_ST_lMain.FindField("DocDate").value = self.DOCDATE
            AP_ST_lMain.FindField("PostDate").value = datetime.datetime.now().strftime('%d/%m/%Y')
            AP_ST_lMain.FindField("DocAmt").AsFloat = self.SL_AMOUNT
            # CONTRA.get('SL_AMOUNT')
            AP_ST_lMain.FindField("Description").AsString = f"From Customer Contra {self.DOCNO}"

            #Knock Off IV  
            V = ["PI", self.PI_DOCNO]  #DocType, DocNo
            if (AP_ST_lDetail.Locate("DocType;DocNo", V, False, False)) :
                AP_ST_lDetail.Edit()
                AP_ST_lDetail.FindField("KOAmt").AsFloat = self.SL_AMOUNT
                # CONTRA.get('SL_AMOUNT')
                AP_ST_lDetail.FindField("KnockOff").AsString = "T"
                AP_ST_lDetail.Post()
        try:
            AP_ST.Save()
            print(f"Posting Supplier Contra {self.DOCNO} Done")
        except Exception as e:
            print("Oops!", e)
            raise e
        AP_ST.Close()


class ContraService:
    @classmethod
    def df(cls, contra_data: List[Dict[str, Any]]) -> pl.DataFrame:
        return (pl.DataFrame(contra_data)
            # .with_row_index(offset=start_index)
            # .with_columns(
            #     DOCDATE = pl.lit(docdate),
            #     DOCNO = pl.col("index").map_elements(lambda x: f"CT-{x:05d}", return_dtype=pl.String),
            # )
            # .drop("index")
        )
        
    @classmethod
    def contra_list(cls, contra_data: List[Dict[str, Any]]) -> List[Contra]:
        df = cls.df(contra_data)
        return [Contra(**row) for row in df.rows(named=True)]
    
    @classmethod
    def query(cls, ComServer: CDispatch, month: int = None, year: int = None, page: int = None, limit: int = None) -> List[Dict[str, Any]]:
        lSQL = f"""SELECT 
            --SL.DOCNO AS DOCNO
            --COUNT(*) AS TOTAL
            SL.DOCNO AS SL_DOCNO, SL.CODE AS SL_CODE, SL.DOCDATE AS SL_DATE,
            PI.DOCNO AS PI_DOCNO, PI.CODE AS PI_CODE, PI.DOCDATE AS PI_DATE,
            PI.DOCAMT AS PI_AMOUNT, SL.DOCAMT AS SL_AMOUNT
        FROM SL_IV SL
        JOIN PH_PI PI 
            --ON SL.DOCDATE = PI.DOCDATE
            ON EXTRACT(MONTH FROM SL.DOCDATE) = EXTRACT(MONTH FROM PI.DOCDATE)
            AND SUBSTRING(SL.CODE FROM 5) = PI.CODE
        """
        where_list = [month, year]
        where_query = []
        if any(where_list):
            lSQL += " WHERE "
            if month:
                where_query.append(f"EXTRACT(MONTH FROM SL.DOCDATE) = {month}")
            if year:
                where_query.append(f"EXTRACT(YEAR FROM SL.DOCDATE) = {year}")
            lSQL += " AND ".join(where_query)
            
        if page and limit:
            lSQL += f" ROWS {(page-1)*limit+1} TO {page*limit}"
            
        lSQL += ";"
        lDataSet = ComServer.DBManager.NewDataSet(lSQL)
        return ComServerService.GetResult(lDataSet)
        
        
# binded = ContraService.contra_list(binded, 2, "31/01/2025")
# binded = binded[1:]
# for r in binded:
#     try:
#         r.post(ComServer)
#     except Exception as e:
#         print("Error", e)
#         # Optional: Log the error or take other actions as needed