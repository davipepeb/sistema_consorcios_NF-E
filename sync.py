"""
sync.py — Backup opcional para Google Sheets.
Importado lazily pelo app.py apenas quando GS_IDs estão configurados.
"""

from __future__ import annotations
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "ID", "Tipo", "DataLancamento", "DataVencimento", "DataCompetencia",
    "NomeContraparte", "CNPJContraparte", "Descricao",
    "ValorBruto", "Deducoes", "ValorLiquido",
    "ISS", "ISSRetido", "Aliquota",
    "Status", "NumeroNF", "CodigoVerificacao", "Observacoes",
]


@st.cache_resource(show_spinner=False)
def _client() -> gspread.Client:
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]), scopes=SCOPES
    )
    return gspread.authorize(creds)


def append_to_sheet(row: dict, spreadsheet_id: str, worksheet_name: str) -> None:
    sh = _client().open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=5000, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option="RAW")

    if not ws.get_all_values():
        ws.append_row(HEADERS, value_input_option="RAW")

    ws.append_row([row.get(h, "") for h in HEADERS], value_input_option="USER_ENTERED")
