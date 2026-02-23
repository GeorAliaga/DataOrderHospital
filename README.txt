Consolidador Hospital App (v4.6.1)

Fix: IndentationError en engine.py (report.append fuera de la función).
No cambia la lógica; solo corrige el error de indentación y actualiza el header del reporte.


v4.7: Corrección basada en salida buena DIC 2025:
- CODIGO: normaliza 00017024 -> 17024 (string), evita floats
- SHIFT: si CODIGO trae SF113/SF112 con Unnamed:1 vacío, lo mueve a Unnamed:1
- CPT: limpieza NO destructiva (conserva letras y decimales) + empaquetado del bloque CPT para que no falten CPT en PROC ENF


Verificación rápida:
  python verificar.py Resultado.xlsx
  python verificar.py Resultado.xlsx "PROCEDIMIENTOS MED Y ENF DIC 2025.xlsx"


v4.8.1: Fix crash 'name context is not defined' (se definió context por hoja antes de apply_column_aliases).


v4.8.2: Fix crash pack_cpt_block() signature (ahora acepta args extra para compatibilidad).


v4.8.3: Mejoras de robustez:
- Detecta headers donde FECHA viene como 'fecha' en el encabezado (datetime) y lo renombra a Unnamed:0.
- Alias UPSS -> UNIDAD DE ORIGEN.
- Relleno parcial de UNIDAD DE ORIGEN (no solo si estaba casi vacío).
- Filtro de filas vacías considera CPT en columnas opcionales (evita perder registros ENF).
