import win32com.client
import pythoncom
from typing import List, Dict, Any
import os
from time import sleep

# pythoncom.CoInitialize()  # Initialize COM in this thread
# ComServer = win32com.client.Dispatch("SQLAcc.BizApp")

def get_comserver():
    pythoncom.CoInitialize()
    return win32com.client.Dispatch("SQLAcc.BizApp")

class ComServerService:
    @classmethod
    def KillApp(cls):
        os.system('cmd /c "taskkill /IM "SQLACC.exe" /F"')
        sleep(2) #sleep 2 sec

    @classmethod
    def CheckLogin(cls):
        ComServer = win32com.client.Dispatch("SQLAcc.BizApp")
        B = ComServer.IsLogin
        if B == True:
            ComServer.Logout()
            cls.KillApp()
        ComServer = win32com.client.Dispatch("SQLAcc.BizApp")
        try:    
            ComServer.Login("ADMIN", "ADMIN", #UserName, Password
                            "\\DESKTOP-C8CJ70C\eStream\SQLAccounting\Share\Default.DCF", #DCF file
                            "ACC-0001.FDB") #Database Name
        except Exception as e:
            print("Oops !", e)

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

    @classmethod
    def QuotedStr(cls, ACode):
        return "'" + ACode.replace("'", "''") + "'"
    
    
    