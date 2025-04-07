from pydantic import BaseModel, Field
import datetime
from typing import List
import requests
import json
import tomllib as tl
from enum import Enum

with open("config.toml", "rb") as f:
    config = tl.load(f)
    ssm = config["SSMERP"]

class LoginSchema(BaseModel):
    client_id: str
    client_secret: str
    grant_type: str = "client_credentials"
    scope: str = "InvoicingAPI"

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    scope: str

class Service:
    base_url: str = "https://preprod-api.myinvois.hasil.gov.my"
    credentials: LoginResponse = None

    @classmethod
    def get(cls, url: str, headers: dict) -> requests.Response:
        if cls.credentials is None:
            raise ValueError("Credentials not found")
        if "Authentication" not in headers:
            headers["Authorization"] = f"{cls.credentials.token_type} {cls.credentials.access_token}"
        return requests.get(cls.base_url + url, headers=headers)

    @classmethod
    def post(cls, url: str, headers: dict, body: dict) -> requests.Response:
        if cls.credentials is None:
            raise ValueError("Credentials not found")
        if "Authentication" not in headers:
            headers["Authorization"] = f"{cls.credentials.token_type} {cls.credentials.access_token}"
        return requests.post(cls.base_url + url, headers=headers, data=body)

    @classmethod
    def login(cls, body: LoginSchema) -> LoginResponse:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        res = requests.post(cls.base_url + "/connect/token", data=body.model_dump(), headers=headers)
        cls.credentials =  LoginResponse(
            **json.loads(res.text)
        )
        return cls.credentials


#region Value Types
class ValueStr_(BaseModel):
    value: str = Field(..., alias="_")
class ValueInt_(BaseModel):
    value: int = Field(..., alias="_")
class ValueFloat_(BaseModel):
    value: float = Field(..., alias="_")
class ValueBool_(BaseModel):
    value: bool = Field(..., alias="_")
class ValueDate_(BaseModel):
    value: datetime.date = Field(..., alias="_")
class ValueDateTime_(BaseModel):
    value: datetime.datetime = Field(..., alias="_")
class ValueTime_(BaseModel):
    value: datetime.time = Field(..., alias="_")
#endregion

#region Invoice Metadata
class InvoiceTypeCode_(BaseModel):
    value: str = Field(default="01", alias="_")
    listVersionID: str = "1.0"
class DocumentCurrencyCode_(BaseModel):
    value: str = Field(default="MYR", alias="_")
class TaxCurrencyCode_(BaseModel):
    value: str = Field(default="MYR", alias="_")
class InvoicePeriod_(BaseModel):
    StartDate: List[ValueDate_]
    EndDate: List[ValueDate_]
    Description: List[ValueStr_]
#endregion

#region Party
class AdditionalAccountID_(ValueStr_):
    schemeAgencyName: str = "CertEX"
class IndustryClassificationCode_(ValueStr_):
    name: str
class PartyIdentificationDetails_(ValueStr_):
    schemeID: str
class PartyIdentification_(BaseModel):
    ID: List[PartyIdentificationDetails_]
class AddressLine_(BaseModel):
    Line: List[ValueStr_]
class IdentificationCode_(ValueStr_):
    value: str = Field("MYS", alias="_")
    listID: str = "ISO3166-1"
    listAgencyID: str = "6"
class Country_(BaseModel):
    IdentificationCode: List[IdentificationCode_]

class PostalAddress_(BaseModel):
    CityName: List[ValueStr_]
    PostalZone: List[ValueStr_]
    CountrySubentityCode: List[ValueStr_]
    AddressLine: List[AddressLine_]
    Country: List[Country_]


class PartyLegalEntity_(BaseModel):
    RegistrationName: List[ValueStr_] = Field(..., alias="RegistrationName")

class Contact_(BaseModel):
    Telephone: List[ValueStr_]
    ElectronicMail: List[ValueStr_] = None

class IndustryClassificationCode_(ValueStr_):
    name: str
    
class PartyDetails_(BaseModel):
    IndustryClassificationCode: List[IndustryClassificationCode_] = None # Mandatory for Seller, Optional for Buyer
    PostalAddress: List[PostalAddress_]
    PartyIdentification: List[PartyIdentification_]
    PartyLegalEntity: List[PartyLegalEntity_]
    Contact: List[Contact_]
    

class Party_(BaseModel):
    AdditionalAccountID: List[AdditionalAccountID_] = None
    Party: List[PartyDetails_]

#endregion

#region Payment Terms
class PaymentModeCode(str, Enum):
    Cash = "01"
    Cheque = "02"
    BankTransfer = "03"
    CreditCard = "04"
    DebitCard = "05"
    EWallet = "06"
    DigitalBank = "07"
    Others = "08"

class PaymentMeans_(BaseModel):
    PaymentMeansCode: List[ValueStr_]
    
class PaymentTerms_(BaseModel):
    Note: List[ValueStr_]

#endregion

#region Tax
class TaxTypeCode(str, Enum):
    SalesTax = "01"
    ServiceTax = "02"
    TourismTax = "03"
    HighValueGoodsTax = "04"
    SalesTaxLowValueGoods = "05"
    NotApplicable = "06"
    TaxExemption = "E"
    
class TaxAmount_(ValueFloat_):
    currencyID: str = "MYR"

class TaxSchemeID_(BaseModel):
    value: str = Field(default="OTH", alias="_")
    schemeID: str = "UN/ECE 5153"
    schemeAgencyID: str = "6"
class TaxScheme_(BaseModel):
    ID: List[TaxSchemeID_] = [TaxSchemeID_()]
class TaxCategory_(BaseModel):
    ID: List[ValueStr_]
    TaxScheme: List[TaxScheme_] = [TaxScheme_()]

    
class TaxSubtotal_(BaseModel):
    TaxableAmount: List[TaxAmount_]
    TaxAmount: List[TaxAmount_]
    Percent: List[ValueInt_] = None
    TaxCategory: List[TaxCategory_]

class TaxTotal_(BaseModel):
    TaxAmount: List[TaxAmount_]
    TaxSubtotal: List[TaxSubtotal_] = None



class LegalMonetaryTotal_(BaseModel):
    # mandatory fields
    TaxExclusiveAmount: List[TaxAmount_]
    TaxInclusiveAmount: List[TaxAmount_]
    PayableAmount: List[TaxAmount_]
    
    # optional fields
    LineExtensionAmount: List[TaxAmount_] | None = None
    AllowanceTotalAmount: List[TaxAmount_] | None = None
    ChargeTotalAmount: List[TaxAmount_] | None = None
    

#endregion

#region Line Items
class ItemClassificationCode(str, Enum):
    BreastfeedingEquipment = "001"
    ChildCareCentresKindergartensFees = "002"
    ComputerSmartphoneTablet = "003"
    ConsolidatedEInvoice = "004"
    ConstructionMaterials = "005"
    Disbursement = "006"
    Donation = "007"
    ECommerceEInvoiceBuyerPurchaser = "008"
    ECommerceSelfBilled = "009"
    EducationFees = "010"
    GoodsOnConsignmentConsignor = "011"
    GoodsOnConsignmentConsignee = "012"
    GymMembership = "013"
    InsuranceEducationMedicalBenefits = "014"
    InsuranceTakafulLifeInsurance = "015"
    InterestFinancingExpenses = "016"
    InternetSubscription = "017"
    LandBuilding = "018"
    MedicalExaminationLearning = "019"
    MedicalExaminationVaccinationExpenses = "020"
    MedicalExpensesSeriousDiseases = "021"
    Others = "022"
    PetroleumOperations = "023"
    PrivateRetirementSchemeDeferredAnnuityScheme = "024"
    MotorVehicle = "025"
    SubscriptionBooks = "026"
    Reimbursement = "027"
    RentalMotorVehicle = "028"
    EVChargingFacilities = "029"
    RepairMaintenance = "030"
    ResearchDevelopment = "031"
    ForeignIncome = "032"
    SelfBilledBettingGaming = "033"
    SelfBilledImportationGoods = "034"
    SelfBilledImportationServices = "035"
    SelfBilledOthers = "036"
    SelfBilledMonetaryPayment = "037"
    SportsEquipmentRentalEntryFees = "038"
    SupportingEquipmentDisabledPerson = "039"
    VoluntaryContributionApprovedProvidentFund = "040"
    DentalExaminationTreatment = "041"
    FertilityTreatment = "042"
    TreatmentHomeCareNursing = "043"
    VouchersGiftCardsLoyaltyPoints = "044"
    SelfBilledNonMonetaryPayment = "045"
    
class ItemClassificationDesc(str, Enum):
    BreastfeedingEquipment = "Breastfeeding equipment"
    ChildCareCentresKindergartensFees = "Child care centres and kindergartens fees"
    ComputerSmartphoneTablet = "Computer, smartphone or tablet"
    ConsolidatedEInvoice = "Consolidated e-Invoice"
    ConstructionMaterials = "Construction materials (as specified under Fourth Schedule of the Lembaga Pembangunan Industri Pembinaan Malaysia Act 1994)"
    Disbursement = "Disbursement"
    Donation = "Donation"
    ECommerceEInvoiceBuyerPurchaser = "e-Commerce - e-Invoice to buyer / purchaser"
    ECommerceSelfBilled = "e-Commerce - Self-billed e-Invoice to seller, logistics, etc."
    EducationFees = "Education fees"
    GoodsOnConsignmentConsignor = "Goods on consignment (Consignor)"
    GoodsOnConsignmentConsignee = "Goods on consignment (Consignee)"
    GymMembership = "Gym membership"
    InsuranceEducationMedicalBenefits = "Insurance - Education and medical benefits"
    InsuranceTakafulLifeInsurance = "Insurance - Takaful or life insurance"
    InterestFinancingExpenses = "Interest and financing expenses"
    InternetSubscription = "Internet subscription"
    LandBuilding = "Land and building"
    MedicalExaminationLearning = "Medical examination for learning disabilities and early intervention or rehabilitation treatments of learning disabilities"
    MedicalExaminationVaccinationExpenses = "Medical examination or vaccination expenses"
    MedicalExpensesSeriousDiseases = "Medical expenses for serious diseases"
    Others = "Others"
    PetroleumOperations = "Petroleum operations (as defined in Petroleum (Income Tax) Act 1967)"
    PrivateRetirementSchemeDeferredAnnuityScheme = "Private retirement scheme or deferred annuity scheme"
    MotorVehicle = "Motor vehicle"
    SubscriptionBooks = "Subscription of books / journals / magazines / newspapers / other similar publications"
    Reimbursement = "Reimbursement"
    RentalMotorVehicle = "Rental of motor vehicle"
    EVChargingFacilities = "EV charging facilities (Installation, rental, sale / purchase or subscription fees)"
    RepairMaintenance = "Repair and maintenance"
    ResearchDevelopment = "Research and development"
    ForeignIncome = "Foreign income"
    SelfBilledBettingGaming = "Self-billed - Bettin and gaming"
    SelfBilledImportationGoods = "Self-billed - Importation of goods"
    SelfBilledImportationServices = "Self-billed - Importation of services"
    SelfBilledOthers = "Self-billed - Others"
    SelfBilledMonetaryPayment = "Self-billed - Monetary payment to agents, dealers or distributors"
    SportsEquipmentRentalEntryFees = "Sports equipment, rental / entry fees for sports facilities, registration in sports competition or sports training fees imposed by associations / sports clubs / companies registered with the Sports Commissioner or Companies Commission of Malaysia and carrying out sports activities as listed under the Sports Development Act 1997"
    SupportingEquipmentDisabledPerson = "Supporting equipment for disabled person"
    VoluntaryContributionApprovedProvidentFund = "Voluntary contribution to approved provident fund"
    DentalExaminationTreatment = "Dental examination or treatment"
    FertilityTreatment = "Fertility treatment"
    TreatmentHomeCareNursing = "Treatment and home care nursing, daycare centres and residential care centers"
    VouchersGiftCardsLoyaltyPoints = "Vouchers, gift cards, loyalty points, etc"
    SelfBilledNonMonetaryPayment = "Self-billed - Non-monetary payment to agents, dealers or distributors"
    
class UnitType(str, Enum):
    tonne = "TNE"
class InvoicedQuantity_(ValueFloat_):
    unitCode: str = "TNE"
class Currency_(ValueFloat_):
    currencyID: str = "MYR"
class ItemClassificationCode_(ValueStr_):
    value: str = Field(ItemClassificationCode.Others.value, alias="_")
    listID: str = "CLASS"
class CommodityClassification_(BaseModel):
    ItemClassificationCode: List[ItemClassificationCode_]
    
class Item_(BaseModel):
    CommodityClassification: List[CommodityClassification_]
    Description: List[ValueStr_]
    
class Price_(BaseModel):
    PriceAmount: List[Currency_]
class ItemPriceExtension_(BaseModel):
    Amount: List[Currency_]

class InvoiceLine_(BaseModel):
    ID: List[ValueStr_] # ID of the line item
    Item: List[Item_] # Item classification code and description
    Price: List[Price_] # Price of the item
    InvoicedQuantity: List[InvoicedQuantity_] # Quantity of the item
    TaxTotal: List[TaxTotal_] # Tax total of the item
    ItemPriceExtension: List[ItemPriceExtension_] # Subtotal of the item
    LineExtensionAmount: List[Currency_] # Total Excluding Tax
#endregion

class Invoice(BaseModel):
    ID: List[ValueStr_]
    IssueDate: List[ValueDate_]
    IssueTime: List[ValueTime_]
    InvoiceTypeCode: List[InvoiceTypeCode_]
    DocumentCurrencyCode: List[DocumentCurrencyCode_]
    TaxCurrencyCode: List[TaxCurrencyCode_] # Currency of tax
    InvoicePeriod: List[InvoicePeriod_] # Period of invoice
    AccountingSupplierParty: List[Party_] # Seller
    AccountingCustomerParty: List[Party_] # Buyer
    PaymentMeans: List[PaymentMeans_] = None # Payment mode
    PaymentTerms: List[PaymentTerms_] = None # Payment terms
    TaxTotal: List[TaxTotal_] # Total tax
    LegalMonetaryTotal: List[LegalMonetaryTotal_] # Total amount
    InvoiceLine: List[InvoiceLine_] # Line items

if __name__ == "__main__":
    #example create in invoice
    inv001 = Invoice(
        ID=[ValueStr_(_="INV001")],
        IssueDate=[ValueDate_(_=datetime.date.today())],
        IssueTime=[ValueTime_(_=datetime.datetime.now().time())],
        InvoiceTypeCode=[InvoiceTypeCode_()],
        DocumentCurrencyCode=[DocumentCurrencyCode_()],
        TaxCurrencyCode=[TaxCurrencyCode_()],
        InvoicePeriod=[InvoicePeriod_(
            StartDate=[ValueDate_(_=datetime.date.today())],
            EndDate=[ValueDate_(_=datetime.date.today())],
            Description=[ValueStr_(_="Invoice Period")]
        )],
        AccountingSupplierParty=[Party_(
            AdditionalAccountID=[AdditionalAccountID_(_="123456")],
            PostalAddress=[PostalAddress_(
                CityName=[ValueStr_(_="Kuala Lumpur")],
                PostalZone=[ValueStr_(_="50450")],
                CountrySubentityCode=[ValueStr_(_="Wilayah Persekutuan")],
                AddressLine=[AddressLine_(Line=[ValueStr_(_="No 1, Jalan 1/1")])],
                Country=[Country_(IdentificationCode=[IdentificationCode_(_="MYS")])]
            )],
            PartyIdentification=[PartyIdentification_(
                ID=[PartyIdentificationDetails_(_="123456", schemeID="MYID")]
            )],
            PartyLegalEntity=[PartyLegalEntity_(RegistrationName=[ValueStr_(_="Company ABC")])],
            Contact=[Contact_(
                Telephone=[ValueStr_(_="0123456789")],
                ElectronicMail=[ValueStr_(_="")] # email
            )],
        )],
        AccountingCustomerParty=[Party_(
            AdditionalAccountID=[AdditionalAccountID_(_="654321")],
            PostalAddress=[PostalAddress_(
                CityName=[ValueStr_(_="Petaling Jaya")],
                PostalZone=[ValueStr_(_="47810")],
                CountrySubentityCode=[ValueStr_(_="Selangor")],
                AddressLine=[AddressLine_(Line=[ValueStr_(_="No 2, Jalan 2/2")])],
                Country=[Country_(IdentificationCode=[IdentificationCode_(_="MYS")])]
            )],
            PartyIdentification=[PartyIdentification_(
                ID=[PartyIdentificationDetails_(_="654321", schemeID="MYID")]
            )],
            PartyLegalEntity=[PartyLegalEntity_(RegistrationName=[ValueStr_(_="Company XYZ")])],
            Contact=[Contact_(
                Telephone=[ValueStr_(_="0123456789")],
                ElectronicMail=[ValueStr_(_="")] # email
            )],
        )],
        PaymentMeans=[PaymentMeans_(PaymentMeansCode=[ValueStr_(_=PaymentModeCode.Cash.value)])],
        PaymentTerms=[PaymentTerms_(Note=[ValueStr_(_="Payment terms")])],
        TaxTotal=[TaxTotal_(TaxAmount=[TaxAmount_(_=0.0)], TaxSubtotal=[TaxSubtotal_(TaxableAmount=[TaxAmount_(_=0.0)], TaxAmount=[TaxAmount_(_=0.0)], TaxCategory=[TaxCategory_(ID=[ValueStr_(_=TaxTypeCode.NotApplicable.value)], TaxScheme=[TaxScheme_(ID=[TaxSchemeID_(_="MYGST")])])])])],
        LegalMonetaryTotal=[LegalMonetaryTotal_(TaxExclusiveAmount=[TaxAmount_(_=0.0)], TaxInclusiveAmount=[TaxAmount_(_=0.0)], PayableAmount=[TaxAmount_(_=0.0)])],
        InvoiceLine=[InvoiceLine_(
            ID=[ValueInt_(_=1)],
            Item=[Item_(CommodityClassification=[CommodityClassification_(ItemClassificationCode=[ItemClassificationCode_(_=ItemClassificationCode.Others.value)])], Description=[ValueStr_(_="Item 1")])],
            Price=[Price_(PriceAmount=[Currency_(_=100.0)])],
            InvoicedQuantity=[InvoicedQuantity_(_=1.0)],
            TaxTotal=[TaxTotal_(TaxAmount=[TaxAmount_(_=0.0)], TaxSubtotal=[TaxSubtotal_(TaxableAmount=[TaxAmount_(_=0.0)], TaxAmount=[TaxAmount_(_=0.0)], TaxCategory=[TaxCategory_(ID=[ValueStr_(_=TaxTypeCode.NotApplicable.value)], TaxScheme=[TaxScheme_(ID=[TaxSchemeID_(_="MYGST")])])])])],
            ItemPriceExtension=[ItemPriceExtension_(Amount=[Currency_(_=100.0)])],
            LineExtensionAmount=[Currency_(_=100.0)]
        )]        
    )

    from pprint import pprint
    pprint(inv001.model_dump(mode="json", by_alias=True, exclude_none=True))