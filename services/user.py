from typing import List, Literal, Type, Union
from sqlalchemy.orm import Session
from win32com.client import CDispatch
from sqlacc import ComServerService
from database import get_db_session
from models import Supplier, Customer
from pydantic import BaseModel, Field
from sqlalchemy import select, distinct

class UserModel(BaseModel):
    id: int | None = Field(default=None, alias="id")
    code: str = Field(alias="CODE")
    control_ac: str = Field(alias="CONTROLACCOUNT")
    company_name: str = Field(alias="COMPANYNAME")
    second_company_name: str | None = Field(default=None, alias="COMPANYNAME2")
    address_1: str | None = Field(default=None, alias="ADDRESS1")
    address_2: str | None = Field(default=None, alias="ADDRESS2")
    address_3: str | None = Field(default=None, alias="ADDRESS3")
    post_code: str | None = Field(default=None, alias="POSTCODE")
    tin: str | None = Field(default=None, alias="TIN")
    id_type: str | None = Field(default=None, alias="IDTYPE")
    id_no: str | None = Field(default=None, alias="IDNO")

    class Config:
        from_attributes = True

class SupplierModel(UserModel):
    ...
    
class CustomerModel(UserModel):
    ...

class UserService:   
    @classmethod
    def get_code_from_db(cls, session: Session, model: Type[Union[Supplier, Customer]]) -> List[str]:
        stmt = select(distinct(model.code))
        return session.execute(stmt).scalars().all()
    
    @classmethod
    def get_code_from_sqlacc(cls, ComServer: CDispatch, model: Literal['AP_SUPPLIER', 'AR_CUSTOMER']) -> List[str]:
        lSQL = f"SELECT CODE FROM {model}"
        lDataSet = ComServer.DBManager.NewDataSet(lSQL)
        return [row.get("CODE", None) for row in ComServerService.GetResult(lDataSet)]
    
    @classmethod
    def compare(cls, session: Session, ComServer: CDispatch, model: Literal['AP_SUPPLIER', 'AR_CUSTOMER']) -> List[str]:
        db_model = Supplier if model == 'AP_SUPPLIER' else Customer
        db = cls.get_code_from_db(session, db_model)
        sqlacc = cls.get_code_from_sqlacc(ComServer, model)
        
        # Compare the two lists and return the differences
        return list(set(sqlacc) - set(db))
    
    @classmethod
    def insert_unimported(cls, session: Session, ComServer: CDispatch, model: Literal['AP_SUPPLIER', 'AR_CUSTOMER']):
        # Get the list of suppliers from SQLAcc
        # sqlacc = cls.get_code_from_sqlacc(ComServer, model)
        
        # # Get the list of suppliers from the database
        # db_model = Supplier if model == 'AP_SUPPLIER' else Customer
        # db = cls.get_code_from_db(session, db_model)
        
        # Find suppliers that are in SQLAcc but not in the database
        unimported = cls.compare(session, ComServer, model)
        lSQL = f"""SELECT
                    A.CODE, A.CONTROLACCOUNT, A.COMPANYNAME, A.COMPANYNAME2,
                    B.ADDRESS1, B.ADDRESS2, B.ADDRESS3, B.POSTCODE,
                    A.TIN, A.IDTYPE, A.IDNO
                FROM {model} A
                INNER JOIN {model}BRANCH B ON (A.CODE=B.CODE)
                WHERE A.CODE IN ({','.join([f"'{code}'" for code in unimported])});
                """
        lDataSet = ComServer.DBManager.NewDataSet(lSQL)
        binded = ComServerService.GetResult(lDataSet)
        print(f"Unimported suppliers: {unimported}")
        print(f"Binded data: {binded}")
        
        # Insert unimported suppliers into the database
        for rec in binded:
            data = None
            new_model = None
            if model == 'AP_SUPPLIER':
                data = SupplierModel(**rec)
                new_model = Supplier(
                    **data.model_dump(exclude_unset=True)
                )
            elif model == 'AR_CUSTOMER':
                data = CustomerModel(**rec)
                new_model = Customer(
                    **data.model_dump(exclude_unset=True)
                )
            session.add(new_model)
            session.commit()
            print(f"Inserted supplier {rec.get('CODE', None)} into the database.")
        
        # session.commit()

# ComServer = initialize_com()
# # SupplierService.get_suppliers_code_from_sqlacc(ComServer)
# session = next(get_db_session())
# # SupplierService.get_suppliers_code_from_db(session)

# # SupplierService.compare_suppliers(session, ComServer)
# UserService.compare(session, ComServer, 'AP_SUPPLIER')