import os
import re
import unicodedata
import hashlib
from difflib import SequenceMatcher
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import openpyxl


# ============================================================
# Utilidades de normalización
# ============================================================

def _norm_txt(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.upper().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    return s


def normalize_dni(val):
    """Extrae un DNI de 8 dígitos desde cualquier formato común (float/string)."""
    if pd.isna(val):
        return np.nan
    s = str(val).strip()
    s = re.sub(r"\.0$", "", s)
    digs8 = re.findall(r"\d{8}", s)
    if digs8:
        return digs8[0]
    digs = re.findall(r"\d+", s)
    if digs:
        d = digs[0]
        return d[:8] if len(d) >= 8 else np.nan
    return np.nan


def _robust_datetime_from_any(series: pd.Series) -> pd.Series:
    """
    Parse dates robustly from:
    - Excel serial numbers (days since 1899-12-30)
    - strings (dd/mm/yyyy, yyyy-mm-dd, etc.)
    - datetime objects
    Returns datetime64[ns] with NaT for invalid.
    """
    s = series.copy()

    # Numeric -> try Excel serial conversion if looks like a serial date
    num = pd.to_numeric(s, errors="coerce")
    mask_serial = num.notna() & (num.between(20000, 60000))  # ~1954..2064
    dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    if mask_serial.any():
        dt.loc[mask_serial] = pd.to_datetime(num.loc[mask_serial], unit="D", origin="1899-12-30", errors="coerce")

    # Non-serial -> normal parse (dayfirst)
    mask_other = ~mask_serial
    if mask_other.any():
        dt.loc[mask_other] = pd.to_datetime(s.loc[mask_other], errors="coerce", dayfirst=True)

    # Remove typical artifacts
    dt = dt.mask(dt.dt.year == 1970)

    return dt


def standardize_date(series: pd.Series) -> pd.Series:
    """
    Normaliza a dd/mm/yyyy.
    - Soporta datetime, seriales de Excel y strings.
    - Si el valor era solo hora (ej. '08:30'), lo deja en NaN.
    - Si cae en 01/01/1970 (artefacto típico), lo deja en NaN.
    """
    s = series.copy()

    # time-only string -> NaN
    time_only = s.astype(str).str.match(r"^\s*\d{1,2}:\d{2}(:\d{2})?\s*$", na=False)

    dt = _robust_datetime_from_any(s)
    dt = dt.mask(time_only)

    out = dt.dt.strftime("%d/%m/%Y")
    out = out.where(~dt.isna(), np.nan)
    return out


def enforce_turno(series: pd.Series) -> pd.Series:
    """
    En la plantilla, TURNO debería ser M o T.
    Si viene 'MAÑANA/MANANA' -> M, 'TARDE' -> T.
    Otros valores (L-M-V, M-J-S, GN, GD, etc.) se dejan en blanco.
    """
    s = series.astype(str).str.strip().str.upper()
    s = s.replace({"MAÑANA": "M", "MANANA": "M", "TARDE": "T"})
    s = s.where(s.isin(["M", "T"]), np.nan)
    return s


# ============================================================
# Detección de header "plantilla-compatible"
# ============================================================

def find_strict_header_row_openpyxl(ws, max_rows=60) -> Optional[int]:
    """
    Devuelve header_row (0-based) si existe una fila donde aparezcan
    ambos tokens: TURNO y CODIGO.
    """
    for r_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows, values_only=True), start=0):
        vals = [_norm_txt(v) for v in row]
        sset = {v for v in vals if v}
        if "TURNO" in sset and "CODIGO" in sset:
            return r_idx
    return None


def detect_header_row_fallback_openpyxl(ws, max_rows=80) -> Optional[int]:
    """
    Fallback por score de tokens (para formatos no estándar).
    NOTA: si el archivo tiene una hoja buena con TURNO+CODIGO, NO se usa esto.
    """
    best_row, best_score = 0, -1
    for r_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows, values_only=True), start=0):
        vals = [_norm_txt(v) for v in row]
        joined = " ".join(vals)
        score = sum(
            1 for t in ("TURNO","CODIGO","CATEGORIA","DNI","PROCEDIMIENTO","DIAGNOSTICO","FECHA","UBIGEO")
            if t in joined
        )
        if score > best_score:
            best_score, best_row = score, r_idx
    return None if best_score < 2 else best_row
def map_columns_to_template(df_cols: List[str], template_cols: List[str], fuzzy_threshold: float = 0.86) -> dict:
    """
    Mapea columnas del DF al nombre exacto de columna de la plantilla:
    - match exacto normalizado
    - fuzzy match (no Unnamed)
    """
    tpl_norm = {c: _norm_txt(c) for c in template_cols}
    mapping, used = {}, set()

    # exact
    for c in df_cols:
        nc = _norm_txt(c)
        for t, nt in tpl_norm.items():
            if nt and nt == nc and t not in used:
                mapping[c] = t
                used.add(t)
                break

    # fuzzy
    for c in df_cols:
        if c in mapping:
            continue
        nc = _norm_txt(c)
        if not nc or nc.startswith("UNNAMED"):
            continue

        best, best_score = None, 0.0
        for t in template_cols:
            if t in used:
                continue
            nt = tpl_norm[t]
            if not nt or nt.startswith("UNNAMED"):
                continue
            score = SequenceMatcher(None, nc, nt).ratio()
            if score > best_score:
                best_score, best = score, t

        if best is not None and best_score >= fuzzy_threshold:
            mapping[c] = best
            used.add(best)

    return mapping


# ============================================================
# Limpieza / reparación de columnas
# ============================================================

def drop_garbage_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").copy()

    # headers repetidos dentro del cuerpo
    if "TURNO" in df.columns:
        df = df[df["TURNO"].astype(str).str.upper().ne("TURNO")]

    # quitar filas completamente vacías en campos clave
    key_candidates = [c for c in [
        "Unnamed: 0", "TURNO", "CODIGO", "DNI DEL PACIENTE",
        "DIAGNOSTICO 1 (CIE 10)", "PROCEDIMIENTO   (CPT)"
    ] if c in df.columns]
    if key_candidates:
        df = df[~df[key_candidates].isna().all(axis=1)].copy()

    return df


def numeric_ratio(series: pd.Series, sample=400) -> float:
    s = series.dropna()
    if len(s) == 0:
        return 0.0
    if len(s) > sample:
        s = s.sample(sample, random_state=0)
    st = s.astype(str).str.strip()
    return st.str.match(r"^\d+(\.\d+)?$").mean()


def fix_procedure_pairs(df: pd.DataFrame, template_cols: List[str]) -> pd.DataFrame:
    """
    En varias plantillas, el CPT/código cae en la columna Unnamed contigua.
    Si detecta que el "código" está en Unnamed y el texto en PROCEDIMIENTO -> swap.
    """
    df = df.copy()
    for i, col in enumerate(template_cols[:-1]):
        nxt = template_cols[i + 1]
        if (
            isinstance(col, str) and col.startswith("PROCEDIMIENTO")
            and isinstance(nxt, str) and nxt.startswith("Unnamed")
            and col in df.columns and nxt in df.columns
        ):
            r1 = numeric_ratio(df[col])
            r2 = numeric_ratio(df[nxt])
            if r1 < 0.35 and r2 > 0.55:
                df[col], df[nxt] = df[nxt], df[col]
    return df


# ============================================================
# Bases de datos (corrección)
# ============================================================

def load_databases(pacientes_xlsx: str, personal_xlsx: str):
    pac = pd.read_excel(pacientes_xlsx)
    if "DNI" in pac.columns:
        pac["DNI"] = pac["DNI"].apply(normalize_dni)
    pac = pac.dropna(subset=["DNI"]).drop_duplicates(subset=["DNI"], keep="first")
    pac_lookup = pac.set_index("DNI").to_dict("index")

    per = pd.read_excel(personal_xlsx, header=2)
    per = per[per["Unnamed: 0"].astype(str).str.strip().ne("N°")].copy()
    per = per.rename(columns={
        "Unnamed: 1": "GRADO",
        "Unnamed: 2": "ESPECIALIDAD",
        "Unnamed: 3": "APELLIDOS_Y_NOMBRES",
        "Unnamed: 5": "DNI",
    })
    per["DNI"] = per["DNI"].apply(normalize_dni)
    per = per.dropna(subset=["DNI"]).drop_duplicates(subset=["DNI"], keep="first")
    per_lookup = per.set_index("DNI").to_dict("index")

    return pac_lookup, per_lookup


def enrich_with_dbs(df: pd.DataFrame, pac_lookup, per_lookup) -> pd.DataFrame:
    df = df.copy()

    # Paciente
    if "DNI DEL PACIENTE" in df.columns:
        df["DNI DEL PACIENTE"] = df["DNI DEL PACIENTE"].apply(normalize_dni)

    if "DNI DEL TITULAR" in df.columns:
        df["DNI DEL TITULAR"] = df["DNI DEL TITULAR"].apply(normalize_dni)

    if "Edad" in df.columns and "DNI DEL PACIENTE" in df.columns:
        df["Edad"] = df["Edad"].astype("object")
        miss = df["Edad"].isna() | (df["Edad"].astype(str).str.strip() == "")
        df.loc[miss, "Edad"] = df.loc[miss, "DNI DEL PACIENTE"].map(
            lambda d: pac_lookup.get(d, {}).get("edad") if pd.notna(d) else np.nan
        )

    if "Sexo" in df.columns and "DNI DEL PACIENTE" in df.columns:
        df["Sexo"] = df["Sexo"].astype("object")
        miss = df["Sexo"].isna() | (df["Sexo"].astype(str).str.strip() == "")
        df.loc[miss, "Sexo"] = df.loc[miss, "DNI DEL PACIENTE"].map(
            lambda d: pac_lookup.get(d, {}).get("Sexo") if pd.notna(d) else np.nan
        )

    # Personal: si existe DNI del personal en la columna Unnamed:9, corrige nombre/especialidad
    if "Unnamed: 9" in df.columns:
        df["Unnamed: 9"] = df["Unnamed: 9"].apply(normalize_dni)

        name_col = "NOMBRE DEL MEDICO / PERSONAL DE SALUD TRATANTE"
        esp_col = "ESPECIALIDAD DE MEDICO TRATANTE"

        if name_col in df.columns:
            df[name_col] = df.apply(
                lambda r: per_lookup.get(r["Unnamed: 9"], {}).get("APELLIDOS_Y_NOMBRES")
                if pd.notna(r.get("Unnamed: 9")) and r.get("Unnamed: 9") in per_lookup
                else r.get(name_col),
                axis=1
            )

        if esp_col in df.columns:
            df[esp_col] = df.apply(
                lambda r: per_lookup.get(r["Unnamed: 9"], {}).get("ESPECIALIDAD")
                if pd.notna(r.get("Unnamed: 9")) and r.get("Unnamed: 9") in per_lookup
                else r.get(esp_col),
                axis=1
            )

    return df


# ============================================================
# Duplicados entre hojas del mismo archivo (rápido)
# ============================================================


def _decat_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert any categorical columns to plain object to avoid sort errors."""
    for c in df.columns:
        try:
            if pd.api.types.is_categorical_dtype(df[c]):
                df[c] = df[c].astype("object")
        except Exception:
            pass
    return df
def quick_fingerprint(df: pd.DataFrame, key_cols: List[str], sample_each=120) -> str:
    """
    Huella rápida: hash de tuplas (primeras/últimas filas) en columnas clave.
    Evita comparar DF completos (caro).
    """
    if not key_cols:
        # fallback: shape+columns
        base = f"{df.shape}|{','.join(map(str, df.columns))}"
        return hashlib.md5(base.encode("utf-8")).hexdigest()

    sub = df[key_cols].copy()
    for c in key_cols:
        sub[c] = sub[c].astype(str).replace("nan", "").str.strip()

    head = sub.head(sample_each)
    tail = sub.tail(sample_each)
    mix = pd.concat([head, tail], ignore_index=True)

    joined = "\n".join("|".join(map(str, row)) for row in mix.itertuples(index=False, name=None))
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


# ============================================================
# Escritura en plantilla (sin romper formato)
# ============================================================

def _find_last_nonempty_row(ws, start_row=3) -> int:
    max_r = ws.max_row
    max_c = ws.max_column
    for r in range(max_r, start_row - 1, -1):
        if any(ws.cell(r, c).value is not None for c in range(1, max_c + 1)):
            return r
    return start_row - 1


def _clear_data_area(ws, start_row=3):
    last = _find_last_nonempty_row(ws, start_row)
    if last < start_row:
        return
    for r in range(start_row, last + 1):
        for c in range(1, ws.max_column + 1):
            ws.cell(r, c).value = None


def _write_df(ws, df: pd.DataFrame, start_row=3):
    for i, row in enumerate(df.itertuples(index=False), start=start_row):
        for j, val in enumerate(row, start=1):
            if isinstance(val, float) and np.isnan(val):
                val = None
            ws.cell(i, j).value = val


# ============================================================
# Procesamiento por grupo (MED y ENF por separado)
# ============================================================

def _process_group(paths: List[str], template_cols: List[str], pac_lookup, per_lookup, report: List[str], group_name: str, sort_output: bool) -> pd.DataFrame:
    parts = []

    for path in paths:
        try:
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True, keep_links=False)
        except Exception as e:
            report.append(f"[{group_name}] [ERR] {os.path.basename(path)} | No se pudo abrir: {type(e).__name__}")
            continue

        # elegir SOLO hojas con header estricta (TURNO+CODIGO) si existen
        strict_sheets: List[Tuple[str, int]] = []
        fallback_sheets: List[Tuple[str, int]] = []

        for sh in wb.sheetnames:
            ws = wb[sh]
            hr = find_strict_header_row_openpyxl(ws)
            if hr is not None:
                strict_sheets.append((sh, hr))
            else:
                fr = detect_header_row_fallback_openpyxl(ws)
                if fr is not None:
                    fallback_sheets.append((sh, fr))

        chosen = strict_sheets if strict_sheets else fallback_sheets

        # si hay strict, NO procesar las fallback (evita hojas resumen tipo "ENERO" que rompen PROC ENF)
        if strict_sheets:
            for sh, fr in fallback_sheets:
                report.append(f"[{group_name}] [SKIP] {os.path.basename(path)} :: {sh} | Hoja resumen/no-plantilla (hay otra hoja con TURNO+CODIGO en el mismo archivo)")

        seen_fp = set()
        for sh, hr in chosen:
            try:
                df = pd.read_excel(path, sheet_name=sh, header=hr)
            except Exception as e:
                report.append(f"[{group_name}] [ERR] {os.path.basename(path)} :: {sh} | ReadError {type(e).__name__}")
                continue

            # Mapear columnas al esquema de la plantilla (reduce vacíos)
            mapping = map_columns_to_template(list(df.columns), template_cols)
            if mapping:
                df = df.rename(columns=mapping)

            in_rows = len(df)
            df = drop_garbage_rows(df)

            # estandarizaciones clave
            if "Unnamed: 0" in df.columns:
                df["Unnamed: 0"] = standardize_date(df["Unnamed: 0"])
            if "TURNO" in df.columns:
                df["TURNO"] = enforce_turno(df["TURNO"])

            df = enrich_with_dbs(df, pac_lookup, per_lookup)
            df = fix_procedure_pairs(df, template_cols)

            # alinear a plantilla (conserva columnas exactas)
            df = df.reindex(columns=template_cols)

            # fingerprint para duplicados dentro del mismo archivo (ej. Hoja1 y "1-31 ENERO 2026")
            key_cols = [c for c in ["Unnamed: 0","TURNO","CODIGO","DNI DEL PACIENTE","PROCEDIMIENTO   (CPT)","DIAGNOSTICO 1 (CIE 10)"] if c in df.columns]
            fp = quick_fingerprint(df.dropna(how="all"), key_cols)
            if fp in seen_fp:
                report.append(f"[{group_name}] [SKIP] {os.path.basename(path)} :: {sh} | Duplicado (misma data en otra hoja del archivo) | in={in_rows} out={len(df)}")
                continue
            seen_fp.add(fp)

            report.append(f"[{group_name}] [OK] {os.path.basename(path)} :: {sh} | in={in_rows} out={len(df)}")
            parts.append(df)

        wb.close()

    if not parts:
        return pd.DataFrame(columns=template_cols)

    out = pd.concat(parts, ignore_index=True)
    out = _decat_df(out)

    # quitar duplicados entre archivos por una clave razonable (sin perder información útil)
    dedup_cols = [c for c in ["Unnamed: 0","TURNO","CODIGO","DNI DEL PACIENTE","PROCEDIMIENTO   (CPT)","DIAGNOSTICO 1 (CIE 10)"] if c in out.columns]
    if dedup_cols:
        out = out.drop_duplicates(subset=dedup_cols, keep="first")

    # ordenar (para que no salga “mezclado”)
    if sort_output and "Unnamed: 0" in out.columns:
        dtp = _robust_datetime_from_any(out["Unnamed: 0"])
        out["_sort_date"] = dtp

        if "TURNO" in out.columns:
            out["_sort_turno"] = out["TURNO"].map({"M": 0, "T": 1})
        else:
            out["_sort_turno"] = np.nan

        if "CODIGO" in out.columns:
            out["_sort_codigo"] = pd.to_numeric(out["CODIGO"], errors="coerce")
        else:
            out["_sort_codigo"] = np.nan

        # Orden estable
        out = out.sort_values(
            by=["_sort_date", "_sort_turno", "_sort_codigo"],
            ascending=[True, True, True],
            na_position="last",
            kind="mergesort",
        )
        out = out.drop(columns=["_sort_date", "_sort_turno", "_sort_codigo"])

    return out


# ============================================================
# API principal
# ============================================================

def consolidate(
    plantilla_xlsx: str,
    pacientes_xlsx: str,
    personal_xlsx: str,
    proc_med_files: List[str],
    proc_enf_files: List[str],
    out_xlsx: str,
    include_audit_sheet: bool = False,
    write_report_txt: bool = True,
    sort_output: bool = True,
):
    """
    - PROC. MED. se llena SOLO con proc_med_files
    - PROC. ENF se llena SOLO con proc_enf_files
    - NO filtra por fecha/año (usa TODO lo que venga)
    - Evita hojas "resumen" que rompen el armado (si existe una hoja compatible con plantilla, usa solo esa)
    - Respeta formato de Plantilla.xlsx (se escribe en el mismo libro)
    - Reporte .txt opcional al lado del output
    """
    tpl_med_cols = pd.read_excel(plantilla_xlsx, sheet_name="PROC. MED.", header=1, nrows=0).columns.tolist()
    tpl_enf_cols = pd.read_excel(plantilla_xlsx, sheet_name="PROC. ENF", header=1, nrows=0).columns.tolist()

    pac_lookup, per_lookup = load_databases(pacientes_xlsx, personal_xlsx)

    report: List[str] = []
    report.append("=== CONSOLIDADOR HOSPITAL - REPORTE (v4) ===")

    med_df = _process_group(proc_med_files or [], tpl_med_cols, pac_lookup, per_lookup, report, "MED", sort_output)
    enf_df = _process_group(proc_enf_files or [], tpl_enf_cols, pac_lookup, per_lookup, report, "ENF", sort_output)

    report.append("")
    report.append(f"PROC. MED. filas: {len(med_df)}")
    report.append(f"PROC. ENF. filas: {len(enf_df)}")

    # Escribir dentro de la plantilla para conservar TODO el formato
    wb = openpyxl.load_workbook(plantilla_xlsx, keep_links=False)
    ws_med = wb["PROC. MED."]
    ws_enf = wb["PROC. ENF"]

    _clear_data_area(ws_med, start_row=3)
    _clear_data_area(ws_enf, start_row=3)

    _write_df(ws_med, med_df, start_row=3)
    _write_df(ws_enf, enf_df, start_row=3)

    # Hoja AUDITORIA (opcional)
    if include_audit_sheet:
        if "AUDITORIA" in wb.sheetnames:
            del wb["AUDITORIA"]
        ws_a = wb.create_sheet("AUDITORIA")
        ws_a.append(["nota"])
        ws_a.append(["AUDITORIA está pensada para logs técnicos; usa el REPORTE.txt para ver qué hojas entraron o se saltaron."])
    else:
        if "AUDITORIA" in wb.sheetnames:
            del wb["AUDITORIA"]

    wb.save(out_xlsx)

    if write_report_txt:
        base, _ = os.path.splitext(out_xlsx)
        report_path = base + "_REPORTE.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(map(str, report)))
