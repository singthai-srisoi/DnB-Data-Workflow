import streamlit as st
from sqlacc import SQLUtils, initialize_com
import polars as pl
import datetime
from typing import Any

st.title("Contra")

@st.cache_data
def get_data(start_date: str | None = None, end_date: str | None = None) -> pl.DataFrame:
    lSQL = "SELECT DOCKEY, DOCNO, DOCDATE, CODE, COMPANYNAME, DOCAMT FROM SL_IV"
    if start_date and end_date is None:
        lSQL += f" WHERE DOCDATE >= '{start_date}'"
    elif start_date is None and end_date:
        lSQL += f" WHERE DOCDATE <= '{end_date}'"
    elif start_date and end_date:
        lSQL += f" WHERE DOCDATE BETWEEN '{start_date}' AND '{end_date}'"
    ComServer = initialize_com()
    lDataSet = ComServer.DBManager.NewDataSet(lSQL)
    res = SQLUtils.GetResult(lDataSet)
    df = pl.DataFrame(res)
    return df



#region Date Range Selector
today = datetime.date.today()
first_day = today.replace(day=1)
d = st.date_input(
    "Select Date Range",
    (first_day, today)
)
if d:
    try:
        st.text(f"Selected Date Range: {d[0]} to {d[1]}")
        st.session_state["SL_IV"] = pl.DataFrame(get_data(d[0], d[1]))
    except Exception as e:
        ...

if isinstance(d, tuple) and len(d) == 2:
    start_date, end_date = d
else:
    start_date, end_date = None, None

if start_date and end_date:
    try:
        st.session_state["SL_IV"] = get_data(start_date, end_date)
    except Exception as e:
        st.error(f"Error loading data: {e}")
#endregion

if "SL_IV" not in st.session_state:
    st.session_state["SL_IV"] = get_data()

# st.table(st.session_state["SL_IV"])
# st.data_editor(st.session_state["SL_IV"])
# AgGrid(st.session_state["SL_IV"])
event = st.dataframe(
    st.session_state["SL_IV"],
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="multi-row",
)
if event:
    if "selection" not in st.session_state:
        st.session_state["selection"] = []
    selected_indices = event.selection.rows
    selected_df = st.session_state["SL_IV"][selected_indices]
    st.session_state["selection"] = selected_df
    st.write("### Selected Rows")
    st.dataframe(selected_df, use_container_width=True,)


def gen_contra() -> dict[str, Any]:
    ...
    

def post_contra(data: dict[str, Any]):
    BizObject = initialize_com().BizObjects.Find("AR_CT")
    lMain = BizObject.DataSets.Find("MainDataSet")
    lDetail = BizObject.DataSets.Find("cdsKnockOff")

    lSQL = """SELECT A.*, B.NEXTNUMBER FROM SY_DOCNO A 
    INNER JOIN SY_DOCNO_DTL B ON (A.DOCKEY=B.PARENTKEY)
    WHERE A.DOCTYPE='CT'
    ;
    """
    lDataSet = initialize_com().DBManager.NewDataSet(lSQL)
    NextNo = lDataSet.FindField('NEXTNUMBER').AsFloat
    NextNo = int(NextNo)
    DOCNO=f"CT-{NextNo:0>5}"

    # set main
    lDate = datetime.datetime(2023, 2, 22, 13, 0)
    lDate.strftime('%m/%d/%Y')
    BizObject.New()
    lMain.FindField("DocNo").AsString = DOCNO
    lMain.FindField("DocDate").value = datetime.datetime.now()
    lMain.FindField("Code").AsString = data.get("Code")   #"300-C0001" #Customer Account
    # lMain.FindField("CompanyName").AsString = "Cash Sales"

