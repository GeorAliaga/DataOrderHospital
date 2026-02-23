import sys, re
import pandas as pd
import numpy as np

def parse_ddmmyyyy(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def looks_like_cpt(s: str) -> bool:
    s = (s or "").strip().upper().replace(",", ".")
    s = re.sub(r"\s+", "", s)
    if s in {"", "NAN", "NONE"}:
        return False
    return bool(re.fullmatch(r"[A-Z]+\d+(\.\d{1,3})?", s) or re.fullmatch(r"\d{3,7}(\.\d{1,3})?", s))

def sheet_stats(df, label):
    out = []
    out.append(f"--- {label} ---")
    out.append(f"Filas: {len(df)} | Columnas: {len(df.columns)}")

    if "Unnamed: 0" in df.columns:
        d = parse_ddmmyyyy(df["Unnamed: 0"])
        out.append(f"Fecha parse NaT: {int(d.isna().sum())} ({d.isna().mean():.1%}) | min={d.min()} | max={d.max()}")

    if "TURNO" in df.columns:
        s = df["TURNO"].astype("object").fillna("").astype(str).str.strip().str.upper()
        bad = ~s.isin(["", "M", "T", "N"])
        out.append(f"Turno inválido: {int(bad.sum())} ({bad.mean():.1%}) | top={s[bad].value_counts().head(5).to_dict()}")

    if "CODIGO" in df.columns:
        s = df["CODIGO"].astype("object").fillna("").astype(str).str.strip()
        bad = (s != "") & (~s.str.match(r"^\d{5}$"))
        out.append(f"CODIGO inválido: {int(bad.sum())} ({bad.mean():.1%}) | top={s[bad].value_counts().head(5).to_dict()}")

    cpt_cols = [c for c in df.columns if isinstance(c, str) and ("CPT" in c.upper()) and ("PROCEDIMIENTO" in c.upper())]
    if cpt_cols and "PROCEDIMIENTO   (CPT)" in df.columns:
        any_cpt = df[cpt_cols].notna().any(axis=1)
        miss_main = df["PROCEDIMIENTO   (CPT)"].isna()
        salvage = any_cpt & miss_main
        out.append(f"CPT principal vacío: {int(miss_main.sum())} ({miss_main.mean():.1%}) | Salvables (hay CPT en opcionales): {int(salvage.sum())}")

    return "\n".join(out)

def main():
    if len(sys.argv) < 2:
        print("Uso: python verificar.py <Resultado.xlsx> [Gold.xlsx]")
        return
    out_xlsx = sys.argv[1]
    gold = sys.argv[2] if len(sys.argv) >= 3 else None

    for sheet in ["PROC. MED.", "PROC. ENF"]:
        try:
            df = pd.read_excel(out_xlsx, sheet_name=sheet, header=1)
        except Exception as e:
            print(f"No se pudo leer {sheet}: {e}")
            continue
        print(sheet_stats(df, sheet))
        print()

    if gold:
        # intenta comparar con hojas típicas del caso DIC 2025
        try:
            gmed = pd.read_excel(gold, sheet_name="PROC. MEDICOS", header=1)
            genf = pd.read_excel(gold, sheet_name="PROC. ENF", header=1)
            print("--- COMPARACIÓN vs GOLD ---")
            print(f"GOLD MED filas: {len(gmed)} | GOLD ENF filas: {len(genf)}")
            print(f"GOLD CPT MED vacíos: {int(gmed['PROCEDIMIENTO   (CPT)'].isna().sum())}")
            print(f"GOLD CPT ENF vacíos: {int(genf['PROCEDIMIENTO   (CPT)'].isna().sum())}")
        except Exception as e:
            print(f"No se pudo comparar con GOLD: {e}")

if __name__ == "__main__":
    main()
