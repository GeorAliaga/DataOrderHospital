# Copilot instructions for DataOrderHospital

This repository provides a GUI and library to consolidate hospital procedure Excel files into a formatted template.

- **Big picture:** The GUI is in [app.py](app.py) and the processing library is in [engine.py](engine.py). The app collects paths for: Plantilla.xlsx, BD Pacientes.xlsx, BD Personal.xlsx, multiple Proc Med/Enf files, and an output path. The heavy lifting is `consolidate()` in `engine.py` which: loads template columns, loads database lookups, processes MED/ENF groups separately, writes data into the template to preserve formatting, and optionally writes a `_REPORTE.txt` next to the output.

- **Run / debug:** Install deps with `pip install -r requirements.txt`. Run GUI with `python app.py`. For headless usage import `consolidate` from `engine` and call it directly:

```py
from engine import consolidate
consolidate('Plantilla.xlsx','bd/BD_PAC.xlsx','bd/BD_PER.xlsx',['proc_med1.xlsx'],['proc_enf1.xlsx'],'Resultado.xlsx')
```

- **Key conventions & expectations**
  - The Plantilla must contain sheets named exactly `PROC. MED.` and `PROC. ENF` and their header row is expected at Excel row 2 (the code reads with `header=1`). See how columns are read in `engine.consolidate`.
  - Header detection for input files looks for both tokens `TURNO` and `CODIGO` (strict) and falls back to a token-score heuristic. Prefer files that include `TURNO`+`CODIGO` to avoid being skipped.
  - `BD Personal.xlsx` is parsed with `header=2` and the code expects certain `Unnamed:` columns that get remapped (see `load_databases`).

- **Important data rules enforced by the code**
  - DNI normalization: `normalize_dni` extracts an 8-digit DNI when possible; missing/invalid DNIs become NaN.
  - Dates: robust Excel serial/string parsing with `_robust_datetime_from_any` and `standardize_date`; the code removes 1970 artifacts.
  - `TURNO` is coerced to `M`/`T` via `enforce_turno` (other values become blank).
  - Column mapping uses exact normalized names first then fuzzy matching with threshold ~0.86 (`map_columns_to_template`).

- **Output & artifacts**
  - The tool writes results directly into the provided `Plantilla.xlsx` to preserve formatting and saves as the `out_xlsx` you provide.
  - If enabled, a report is written next to the output as `<base>_REPORTE.txt`. The GUI enables this by default.
  - Optional `AUDITORIA` sheet is controlled by `include_audit_sheet` (the TXT report is the primary log for which sheets were processed/skipped).

- **Patterns to follow when editing**
  - Preserve the template-writing strategy: modifications should avoid changing the way `openpyxl` writes into the template (the code clears rows and writes in-place to preserve styles).
  - When adding new parsing logic, replicate the `strict vs fallback` sheet-selection approach used in `_process_group` to avoid processing summary sheets.
  - Use `keep_links=False` when loading workbooks (the repo already sets this to avoid slow loads/remote links).

- **Where to look for examples**
  - GUI wiring / user flow: [app.py](app.py)
  - Core logic, normalization, mapping, dedup and file-level heuristics: [engine.py](engine.py)
  - Quick usage notes and changelog: [README.txt](README.txt)

If any of these areas are unclear or you want additional examples (CLI runner, unit test harness for `engine.consolidate`, or annotated flow diagram), tell me which and I will add it.
