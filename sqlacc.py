import win32com.client
from typing import List, Dict, Any
import pythoncom
import os
from time import sleep
import tomllib

with open('config.toml', 'rb') as f:
    sqlacc = tomllib.load(f)['SQLACC']


# Ensure COM is initialized properly
def initialize_com():
    pythoncom.CoInitialize()  # Initialize COM in this thread
    return win32com.client.Dispatch("SQLAcc.BizApp")


ComServer = initialize_com()
class ServerManager:
    @classmethod
    def KillApp(cls):
        os.system('cmd /c "taskkill /IM "SQLACC.exe" /F"')
        sleep(2) #sleep 2 sec
    @classmethod
    def CheckLogin(cls):
        global ComServer
        ComServer = win32com.client.Dispatch("SQLAcc.BizApp")
        B = ComServer.IsLogin
        if B == True:
            ComServer.Logout()
            cls.KillApp()
        ComServer = win32com.client.Dispatch("SQLAcc.BizApp")
        try:    
            ComServer.Login(sqlacc.get('username', ''), sqlacc.get('password', ''), #UserName, Password
                            sqlacc.get('dfc', ''), #DCF file
                            sqlacc.get('fdb', '')) #Database Name
        except Exception as e:
            print("Oops !", e)
            
    @classmethod
    def Get(self):
        return ComServer
            
class SQLUtils:
    @classmethod
    def ShowResult(cls, ADataset):
        if ADataset.RecordCount > 0:
            while not ADataset.eof:
                fc = ADataset.Fields.Count
                for x in range(fc):
                    fn = ADataset.Fields.Items(x).FieldName
                    fv = ADataset.FindField(fn).AsString
                    lresult = "Index : "+ str(x) + " FieldName : " + fn + " Value : " + fv
                    print (lresult)
                print("====")
                ADataset.Next()
        else:
            print ("Record Not Found")
            
    @classmethod
    def GetResult(cls, ADataset) -> List[Dict[str, Any]] | None:
        Result = []
        if ADataset.RecordCount > 0:
            while not ADataset.eof:
                fc = ADataset.Fields.Count
                lData = {}
                for x in range(fc):
                    fn = ADataset.Fields.Items(x).FieldName
                    fv = ADataset.FindField(fn).AsString
                    lData[fn] = fv
                Result.append(lData)
                ADataset.Next()
            return Result
        else:
            return None


# lSQL = "SELECT * FROM AR_CT A "
# lSQL += "INNER JOIN AR_KNOCKOFF B ON (A.DOCKEY = B.DOCKEY)"
# lSQL = """SELECT * FROM AR_CT A 
# INNER JOIN AR_KNOCKOFF B ON (A.DOCKEY = B.DOCKEY)
# ;
# """
# lDataSet = ComServer.DBManager.NewDataSet(lSQL)
# res = SQLUtils.GetResult(lDataSet)
# import pprint
# pprint.pprint(res)

# BizObject = ComServer.BizObjects.Find('AR_CT')
# lDetailDataSet = BizObject.DataSets.Find('cdsKnockOff')
# # Select(Fields, Where, OrderBy, OutputFormat, Delimiter, OutputFile): String; 
# FileName = lDetailDataSet.Select("*", "", "", "AD", ",", "C:\\Users\\D&B Trading\\Downloads\\AR_CT.json")