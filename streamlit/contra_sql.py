import streamlit as st
from sqlacc import ComServerService, get_comserver
from services.contra_sql_service import ContraService, Contra
import datetime
import calendar
import polars as pl
from database import get_db_session
from models.settings import Setting

session = next(get_db_session())
setting: Setting = session.query(Setting).first()
if setting.contra_index is None:
    setting.contra_index = 1
    session.commit()
    session.refresh(setting)

# region cache helpers
@st.cache_data(show_spinner="Loading month list...")
def get_available_months():
    lSQL = "SELECT DISTINCT EXTRACT(MONTH FROM DOCDATE) AS NAME FROM SL_IV"
    lDataSet = get_comserver().DBManager.NewDataSet(lSQL)
    binded = ComServerService.GetResult(lDataSet)
    return sorted(int(r.get("NAME")) for r in binded)


@st.cache_data(show_spinner="Loading year list...")
def get_available_years():
    lSQL = "SELECT DISTINCT EXTRACT(YEAR FROM DOCDATE) AS NAME FROM SL_IV"
    lDataSet = get_comserver().DBManager.NewDataSet(lSQL)
    binded = ComServerService.GetResult(lDataSet)
    return sorted(int(r.get("NAME")) for r in binded)


@st.cache_data(show_spinner="Loading contra data...")
def get_contra_list_df(month: int, year: int) -> pl.DataFrame:
    data = ContraService.query(get_comserver(), month=month, year=year)
    return ContraService.df(data)

# endregion


# region UI inputs
months = get_available_months()
years = get_available_years()

year_box, month_box = st.columns(2)
with year_box:
    selected_year = st.selectbox("Year", years, index=0, key="query_year")
with month_box:
    selected_month = st.selectbox("Month", months, index=0, key="query_month")

# endregion


# region data loading
if selected_year and selected_month:
    contra_df = get_contra_list_df(selected_month, selected_year)

    # Add selection column
    contra_df = contra_df.with_columns(
        pl.lit(True).alias("Select")
    )

    edited_df = st.data_editor(
        contra_df,
        use_container_width=True,
        key="contra_table",
        hide_index=True,
    )

    selected_rows_df = edited_df.filter(pl.col("Select") == True)

    if selected_rows_df.shape[0] > 0:
        st.header("Selected Rows")
        date_box, index_box = st.columns(2)
        with date_box:
            st.text("Document Date")
            _, num_days = calendar.monthrange(selected_year, selected_month)
            docdate_ = st.date_input(
                "Document Date",
                value=datetime.date(selected_year, selected_month, num_days),
                key="docdate_"
            )
        with index_box:
            st.text("Contra Index")
            start_index = st.number_input(
                "Start Index",
                value=setting.contra_index,
                key="start_index"
            )
            
            if start_index:
                setting.contra_index = start_index
                session.commit()
                session.refresh(setting)

        result_df = (selected_rows_df
            .with_row_index(offset=start_index)
            .with_columns(
                DOCNO = pl.col("index").map_elements(
                    lambda x: f"CT-{x:0>5}",
                    return_dtype=pl.String
                ),
                DOCDATE = pl.lit(docdate_.strftime("%d/%m/%Y")),
            )
            .drop(["index", "Select"])
        )

        st.dataframe(result_df, use_container_width=True, hide_index=True)
        # total SL_AMOUNT
        total_amount = result_df.select(
            pl.col("SL_AMOUNT").cast(pl.Float64).sum().alias("Total")
        ).item()
        st.metric("Total Amount", f"{total_amount:.2f}")
        
        migrate_btn = st.button("Migrate", key="migrate")
        if migrate_btn:
            for r in result_df.rows(named=True):
                contra = Contra(**r)
                try:
                    contra.post(get_comserver())
                    st.write(f"Posted {contra.DOCNO} successfully")
                except Exception as e:
                    st.error(f"Error posting {contra.DOCNO}: {e}")
                    continue
            max_docno = result_df.select(pl.col("DOCNO")).to_series().max().split("-")[1]
            max_index = int(max_docno) + 1
            setting.contra_index = max_index
            session.commit()
            session.refresh(setting)
            st.success(f"Successfully posted {result_df.shape[0]} records")
            

# endregion
