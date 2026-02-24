"""
NFS-e Processor — Streamlit App
Padrão ABRASF v2.04
"""

from __future__ import annotations
from typing import Any

import gspread
import pandas as pd
import plotly.express as px
import streamlit as st
import xmltodict
from google.oauth2.service_account import Credentials

# ──────────────────────────────────────────────
# CONSTANTES
# ──────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_HEADERS = [
    "Numero", "CodigoVerificacao", "DataEmissao", "Competencia",
    "PrestadorCNPJ", "PrestadorInscricaoMunicipal", "PrestadorRazaoSocial", "PrestadorNomeFantasia",
    "TomadorCNPJ", "TomadorRazaoSocial",
    "Discriminacao", "ItemListaServico",
    "ValorServicos", "ValorDeducoes", "BaseCalculo", "Aliquota",
    "ValorISS", "IssRetido", "ValorLiquidoNfse", "InformacoesComplementares",
]

# Chaves de session_state para acumular notas entre navegações
CR_KEY = "records_receber"
CP_KEY = "records_pagar"


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def _g(node: Any, *keys: str, default: str = "") -> str:
    cur: Any = node
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return str(cur).strip() if cur not in ({}, [], "") else default


def _strip_ns(d: Any) -> Any:
    if isinstance(d, dict):
        return {k.split(":")[-1]: _strip_ns(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_strip_ns(i) for i in d]
    return d


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].str.replace(",", "."), errors="coerce").fillna(0.0)
    return df


# ──────────────────────────────────────────────
# PARSE XML
# ──────────────────────────────────────────────
def parse_nfse_xml(xml_bytes: bytes) -> list[dict[str, str]]:
    try:
        raw = xmltodict.parse(xml_bytes, process_namespaces=False)
    except Exception as e:
        raise ValueError(f"Falha ao fazer parse do XML: {e}") from e

    raw = _strip_ns(raw)
    root = next(iter(raw.values()))
    lista = root.get("ListaNfse", root)
    comps = lista.get("CompNfse", [])
    if isinstance(comps, dict):
        comps = [comps]

    records: list[dict[str, str]] = []
    for comp in comps:
        try:
            records.append(_extract_comp(comp))
        except Exception as e:
            st.warning(f"Nota ignorada: {e}")
    if not records:
        raise ValueError("Nenhuma nota encontrada no XML.")
    return records


def _extract_comp(comp: dict) -> dict[str, str]:
    nfse     = comp.get("Nfse", comp)
    inf      = nfse.get("InfNfse", nfse)
    val_nfse = inf.get("ValoresNfse", {}) or {}
    decl     = inf.get("DeclaracaoPrestacaoServico", {}) or {}
    inf_decl = decl.get("InfDeclaracaoPrestacaoServico", decl)
    servico  = inf_decl.get("Servico", {}) or {}
    val_serv = servico.get("Valores", {}) or {}
    prest_d  = inf_decl.get("Prestador", {}) or {}
    tomador  = inf_decl.get("TomadorServico", {}) or {}
    tom_id   = tomador.get("IdentificacaoTomador", {}) or {}
    tom_cnpj = tom_id.get("CpfCnpj", {}) or {}
    pr_cnpj  = prest_d.get("CpfCnpj", {}) or {}
    prest_sv = inf.get("PrestadorServico", {}) or {}

    return {
        "Numero":                      _g(inf, "Numero"),
        "CodigoVerificacao":           _g(inf, "CodigoVerificacao"),
        "DataEmissao":                 _g(inf, "DataEmissao"),
        "Competencia":                 _g(inf_decl, "Competencia"),
        "PrestadorCNPJ":               _g(pr_cnpj, "Cnpj") or _g(pr_cnpj, "Cpf"),
        "PrestadorInscricaoMunicipal": _g(prest_d, "InscricaoMunicipal"),
        "PrestadorRazaoSocial":        _g(prest_sv, "RazaoSocial"),
        "PrestadorNomeFantasia":       _g(prest_sv, "NomeFantasia"),
        "TomadorCNPJ":                 _g(tom_cnpj, "Cnpj") or _g(tom_cnpj, "Cpf"),
        "TomadorRazaoSocial":          _g(tomador, "RazaoSocial"),
        "Discriminacao":               _g(servico, "Discriminacao"),
        "ItemListaServico":            _g(servico, "ItemListaServico"),
        "ValorServicos":               _g(val_serv, "ValorServicos"),
        "ValorDeducoes":               _g(val_serv, "ValorDeducoes"),
        "BaseCalculo":                 _g(val_nfse, "BaseCalculo"),
        "Aliquota":                    _g(val_nfse, "Aliquota"),
        "ValorISS":                    _g(val_nfse, "ValorIss"),
        "IssRetido":                   _g(servico, "IssRetido"),
        "ValorLiquidoNfse":            _g(val_nfse, "ValorLiquidoNfse"),
        "InformacoesComplementares":   _g(inf_decl, "InformacoesComplementares"),
    }


# ──────────────────────────────────────────────
# GOOGLE SHEETS
# ──────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _get_gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]), scopes=SCOPES
    )
    return gspread.authorize(creds)


def append_to_sheet(rows: list[dict[str, str]], spreadsheet_id: str, worksheet_name: str) -> None:
    try:
        client = _get_gspread_client()
        sh = client.open_by_key(spreadsheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=5000, cols=len(SHEET_HEADERS))
        if not ws.get_all_values():
            ws.append_row(SHEET_HEADERS, value_input_option="RAW")
        ws.append_rows([[r.get(h, "") for h in SHEET_HEADERS] for r in rows],
                       value_input_option="USER_ENTERED")
    except gspread.exceptions.APIError as e:
        raise RuntimeError(f"Erro Google Sheets API: {e}") from e


# ──────────────────────────────────────────────
# PÁGINAS
# ──────────────────────────────────────────────
def page_upload(label: str, state_key: str, sheet_id: str, ws_name: str, salvar: bool) -> None:
    st.header(f"📂 {label}")

    uploaded = st.file_uploader(
        "Upload de XMLs de NFS-e", type=["xml"],
        accept_multiple_files=True, key=f"up_{state_key}"
    )

    if uploaded:
        all_records: list[dict[str, str]] = []
        errors: list[str] = []
        prog = st.progress(0)

        for i, f in enumerate(uploaded):
            try:
                all_records.extend(parse_nfse_xml(f.read()))
            except ValueError as e:
                errors.append(f"**{f.name}**: {e}")
            prog.progress((i + 1) / len(uploaded))

        prog.empty()

        if errors:
            with st.expander(f"⚠️ {len(errors)} erro(s)", expanded=True):
                for e in errors:
                    st.error(e)

        if all_records:
            # Acumula em session_state
            existing = st.session_state.get(state_key, [])
            # Evita duplicatas pelo número da nota
            existing_nums = {r["Numero"] for r in existing}
            novos = [r for r in all_records if r["Numero"] not in existing_nums]
            st.session_state[state_key] = existing + novos

            st.success(f"✅ {len(novos)} nota(s) nova(s) carregada(s). "
                       f"Total em memória: {len(st.session_state[state_key])}")

            if salvar:
                if not sheet_id:
                    st.warning("Configure o ID da planilha na aba **Configurações**.", icon="⚠️")
                else:
                    try:
                        append_to_sheet(novos, sheet_id, ws_name)
                        st.success("📊 Dados enviados ao Google Sheets.")
                    except RuntimeError as e:
                        st.error(str(e))

    records = st.session_state.get(state_key, [])
    if records:
        df = pd.DataFrame(records, columns=SHEET_HEADERS)
        st.subheader(f"Notas em memória — {len(df)}")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "⬇️ Exportar CSV",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"nfse_{state_key}.csv",
            mime="text/csv",
        )
        if st.button("🗑️ Limpar notas da memória", key=f"clear_{state_key}"):
            st.session_state[state_key] = []
            st.rerun()


def page_dashboard() -> None:
    st.header("📊 Dashboard Consolidado")

    cr = st.session_state.get(CR_KEY, [])
    cp = st.session_state.get(CP_KEY, [])

    if not cr and not cp:
        st.info("Nenhuma nota carregada. Faça upload em **Contas a Receber** ou **Contas a Pagar**.")
        return

    num_cols = ["ValorServicos", "ValorISS", "BaseCalculo", "ValorDeducoes", "ValorLiquidoNfse"]

    df_cr = _to_numeric(pd.DataFrame(cr, columns=SHEET_HEADERS), num_cols) if cr else pd.DataFrame(columns=SHEET_HEADERS)
    df_cp = _to_numeric(pd.DataFrame(cp, columns=SHEET_HEADERS), num_cols) if cp else pd.DataFrame(columns=SHEET_HEADERS)

    # ── KPIs globais ──
    st.subheader("Visão Geral")
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("NF Receber",     len(df_cr))
    k2.metric("NF Pagar",       len(df_cp))
    k3.metric("Receita Bruta",  f"R$ {df_cr['ValorServicos'].sum():,.2f}")
    k4.metric("Custo (Pagar)",  f"R$ {df_cp['ValorServicos'].sum():,.2f}")
    saldo = df_cr["ValorServicos"].sum() - df_cp["ValorServicos"].sum()
    k5.metric("Saldo",          f"R$ {saldo:,.2f}", delta=f"{saldo:,.2f}")
    k6.metric("ISS Total",      f"R$ {(df_cr['ValorISS'].sum() + df_cp['ValorISS'].sum()):,.2f}")

    st.divider()

    # ── Gráficos lado a lado ──
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Receber — por Tomador")
        if not df_cr.empty and df_cr["TomadorRazaoSocial"].ne("").any():
            agg = (df_cr.groupby("TomadorRazaoSocial", as_index=False)["ValorServicos"]
                   .sum().sort_values("ValorServicos", ascending=False).head(10))
            fig = px.bar(agg, x="ValorServicos", y="TomadorRazaoSocial", orientation="h",
                         text_auto=".2s", color="ValorServicos", color_continuous_scale="Blues",
                         labels={"ValorServicos": "R$", "TomadorRazaoSocial": ""})
            fig.update_layout(showlegend=False, yaxis={"categoryorder": "total ascending"},
                              margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de receber.")

    with col2:
        st.markdown("#### Pagar — por Prestador")
        if not df_cp.empty and df_cp["PrestadorRazaoSocial"].ne("").any():
            agg2 = (df_cp.groupby("PrestadorRazaoSocial", as_index=False)["ValorServicos"]
                    .sum().sort_values("ValorServicos", ascending=False).head(10))
            fig2 = px.bar(agg2, x="ValorServicos", y="PrestadorRazaoSocial", orientation="h",
                          text_auto=".2s", color="ValorServicos", color_continuous_scale="Reds",
                          labels={"ValorServicos": "R$", "PrestadorRazaoSocial": ""})
            fig2.update_layout(showlegend=False, yaxis={"categoryorder": "total ascending"},
                               margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Sem dados de pagar.")

    # ── Série temporal ──
    st.markdown("#### Faturamento Mensal")
    frames = []
    for df_t, lbl in [(df_cr, "Receber"), (df_cp, "Pagar")]:
        if df_t.empty:
            continue
        tmp = df_t.copy()
        tmp["DataParsed"] = pd.to_datetime(tmp["DataEmissao"], errors="coerce")
        tmp = tmp.dropna(subset=["DataParsed"])
        if tmp.empty:
            continue
        tmp["Mes"] = tmp["DataParsed"].dt.to_period("M").astype(str)
        agg_t = tmp.groupby("Mes", as_index=False)["ValorServicos"].sum()
        agg_t["Tipo"] = lbl
        frames.append(agg_t)

    if frames:
        df_ts = pd.concat(frames)
        fig3 = px.line(df_ts, x="Mes", y="ValorServicos", color="Tipo", markers=True,
                       labels={"Mes": "Mês", "ValorServicos": "R$", "Tipo": ""},
                       color_discrete_map={"Receber": "#1f77b4", "Pagar": "#d62728"})
        st.plotly_chart(fig3, use_container_width=True)

    # ── ISS Retido ──
    col3, col4 = st.columns(2)
    for df_t, lbl, col in [(df_cr, "Receber", col3), (df_cp, "Pagar", col4)]:
        with col:
            st.markdown(f"#### ISS Retido — {lbl}")
            if not df_t.empty and "IssRetido" in df_t.columns:
                df_t = df_t.copy()
                df_t["IssLabel"] = df_t["IssRetido"].map({"1": "Retido", "2": "Não Retido"}).fillna("N/D")
                fig_p = px.pie(df_t, names="IssLabel", values="ValorServicos",
                               color_discrete_sequence=px.colors.sequential.Blues_r if lbl == "Receber"
                               else px.colors.sequential.Reds_r)
                st.plotly_chart(fig_p, use_container_width=True)


def page_calculadora() -> None:
    st.header("🧮 Calculadora de Comissão")
    st.caption("Modelo base de comissionamento para corretoras de seguros")

    with st.expander("ℹ️ Como funciona o modelo", expanded=False):
        st.markdown("""
        **Fluxo típico de repasse em corretoras:**
        1. A **seguradora** paga comissão bruta à **corretora master/angariadora**.
        2. A corretora master repassa uma fração ao **corretor parceiro** (sub-repasse).
        3. Sobre o repasse, podem incidir **ISS** (retido na fonte) e **IRRF** (se PF).
        4. O valor líquido recebido pelo corretor é a base do cálculo de DAS/carnê-leão.
        """)

    st.subheader("1. Dados da Apólice / Prêmio")
    c1, c2, c3 = st.columns(3)
    premio      = c1.number_input("Prêmio Líquido (R$)", min_value=0.0, value=10000.0, step=100.0, format="%.2f")
    comissao_pct= c2.number_input("Comissão da Seguradora (%)", min_value=0.0, max_value=100.0, value=20.0, step=0.5, format="%.2f")
    repasse_pct = c3.number_input("Repasse ao Corretor (%)", min_value=0.0, max_value=100.0, value=50.0, step=1.0, format="%.2f",
                                   help="% da comissão bruta repassada ao corretor parceiro")

    st.subheader("2. Tributos sobre o Repasse")
    c4, c5, c6 = st.columns(3)
    iss_pct  = c4.number_input("ISS (%)", min_value=0.0, max_value=10.0, value=5.0, step=0.5, format="%.2f")
    irrf_pct = c5.number_input("IRRF — somente PF (%)", min_value=0.0, max_value=27.5, value=0.0, step=0.5, format="%.2f",
                                help="Aplica-se quando o corretor é Pessoa Física e o valor supera R$ 6.000/mês")
    iss_retido = c6.checkbox("ISS retido na fonte pela tomadora?", value=True)

    st.subheader("3. Parâmetros Adicionais")
    c7, c8 = st.columns(2)
    qtd_apolices = c7.number_input("Quantidade de Apólices no Período", min_value=1, value=1, step=1)
    reducao_bc   = c8.number_input("Redução da Base de Cálculo do ISS (%)", min_value=0.0, max_value=100.0, value=0.0, step=5.0,
                                    help="Ex: 40% conforme Lei Municipal — ISS incide sobre 60% do valor")

    # ── Cálculos ──
    comissao_bruta   = premio * (comissao_pct / 100)
    valor_repasse    = comissao_bruta * (repasse_pct / 100)
    base_iss         = valor_repasse * (1 - reducao_bc / 100)
    valor_iss        = base_iss * (iss_pct / 100)
    valor_irrf       = valor_repasse * (irrf_pct / 100)
    deducoes         = (valor_iss if iss_retido else 0) + valor_irrf
    valor_liquido    = valor_repasse - deducoes
    retencao_master  = comissao_bruta - valor_repasse

    # Por período
    rep_periodo      = valor_repasse   * qtd_apolices
    liq_periodo      = valor_liquido   * qtd_apolices
    iss_periodo      = valor_iss       * qtd_apolices
    master_periodo   = retencao_master * qtd_apolices

    st.divider()
    st.subheader("📋 Resultado — por Apólice")

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Comissão Bruta",     f"R$ {comissao_bruta:,.2f}")
    r2.metric("Repasse Bruto",      f"R$ {valor_repasse:,.2f}")
    r3.metric("Deduções (ISS+IRRF)",f"R$ {deducoes:,.2f}")
    r4.metric("💰 Repasse Líquido", f"R$ {valor_liquido:,.2f}")

    st.subheader(f"📋 Resultado — {qtd_apolices} Apólice(s)")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Repasse Bruto Total",   f"R$ {rep_periodo:,.2f}")
    p2.metric("ISS Total",             f"R$ {iss_periodo:,.2f}")
    p3.metric("Retenção da Master",    f"R$ {master_periodo:,.2f}")
    p4.metric("💰 Líquido do Período", f"R$ {liq_periodo:,.2f}")

    # Gráfico de composição
    st.subheader("Composição do Repasse (por apólice)")
    partes = {
        "Líquido Corretor":    valor_liquido,
        "ISS":                 valor_iss,
        "IRRF":                valor_irrf,
        "Retenção Master":     retencao_master,
    }
    df_pizza = pd.DataFrame({"Parcela": list(partes.keys()), "Valor": list(partes.values())})
    df_pizza = df_pizza[df_pizza["Valor"] > 0]
    fig = px.pie(df_pizza, names="Parcela", values="Valor",
                 color_discrete_sequence=px.colors.qualitative.Set2,
                 hole=0.4)
    fig.update_traces(textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

    # Tabela de memória de cálculo
    with st.expander("🔍 Memória de Cálculo"):
        calc = {
            "Prêmio Líquido":              f"R$ {premio:,.2f}",
            f"Comissão ({comissao_pct}%)": f"R$ {comissao_bruta:,.2f}",
            f"Repasse ({repasse_pct}% da comissão)": f"R$ {valor_repasse:,.2f}",
            f"Base ISS (após redução {reducao_bc}%)": f"R$ {base_iss:,.2f}",
            f"ISS ({iss_pct}% s/ base)":  f"R$ {valor_iss:,.2f}",
            f"IRRF ({irrf_pct}%)":         f"R$ {valor_irrf:,.2f}",
            "Repasse Líquido":             f"R$ {valor_liquido:,.2f}",
        }
        for k, v in calc.items():
            st.markdown(f"- **{k}:** {v}")


def page_configuracoes() -> None:
    st.header("⚙️ Configurações")

    st.subheader("📥 Contas a Receber")
    st.text_input("ID da Planilha", key="cr_sheet_id",
                  help="URL: /spreadsheets/d/**<ID>**/edit")
    st.text_input("Nome da Aba", key="cr_ws_name", value="NFS-e Receber" if "cr_ws_name" not in st.session_state else st.session_state["cr_ws_name"])
    st.toggle("Salvar automaticamente no Sheets", key="cr_save")

    st.divider()

    st.subheader("📤 Contas a Pagar")
    st.text_input("ID da Planilha", key="cp_sheet_id")
    st.text_input("Nome da Aba", key="cp_ws_name", value="NFS-e Pagar" if "cp_ws_name" not in st.session_state else st.session_state["cp_ws_name"])
    st.toggle("Salvar automaticamente no Sheets", key="cp_save")

    st.divider()
    st.subheader("🗂️ Dados em Memória")
    cr_count = len(st.session_state.get(CR_KEY, []))
    cp_count = len(st.session_state.get(CP_KEY, []))
    st.info(f"**Receber:** {cr_count} nota(s)  |  **Pagar:** {cp_count} nota(s)")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ Limpar Receber", use_container_width=True):
            st.session_state[CR_KEY] = []
            st.success("Notas de Receber removidas.")
    with col2:
        if st.button("🗑️ Limpar Pagar", use_container_width=True):
            st.session_state[CP_KEY] = []
            st.success("Notas de Pagar removidas.")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main() -> None:
    st.set_page_config(page_title="NFS-e Processor", page_icon="🧾", layout="wide")

    # Inicializa defaults de session_state
    for key, default in [
        (CR_KEY, []), (CP_KEY, []),
        ("cr_sheet_id", ""), ("cr_ws_name", "NFS-e Receber"), ("cr_save", False),
        ("cp_sheet_id", ""), ("cp_ws_name", "NFS-e Pagar"),  ("cp_save", False),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    with st.sidebar:
        st.image("https://img.icons8.com/fluency/48/invoice.png", width=48)
        st.title("NFS-e Processor")
        st.divider()
        page = st.radio(
            "Navegação",
            ["📊 Dashboard", "📥 Contas a Receber", "📤 Contas a Pagar",
             "🧮 Calculadora", "⚙️ Configurações"],
            label_visibility="collapsed",
        )
        st.divider()
        cr = len(st.session_state.get(CR_KEY, []))
        cp = len(st.session_state.get(CP_KEY, []))
        st.caption(f"📥 {cr} nota(s) a receber")
        st.caption(f"📤 {cp} nota(s) a pagar")

    if page == "📊 Dashboard":
        page_dashboard()
    elif page == "📥 Contas a Receber":
        page_upload(
            "Contas a Receber", CR_KEY,
            st.session_state["cr_sheet_id"],
            st.session_state["cr_ws_name"],
            st.session_state["cr_save"],
        )
    elif page == "📤 Contas a Pagar":
        page_upload(
            "Contas a Pagar", CP_KEY,
            st.session_state["cp_sheet_id"],
            st.session_state["cp_ws_name"],
            st.session_state["cp_save"],
        )
    elif page == "🧮 Calculadora":
        page_calculadora()
    elif page == "⚙️ Configurações":
        page_configuracoes()


if __name__ == "__main__":
    main()