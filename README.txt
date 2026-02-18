Consolidador Hospital (v4)

Instalar:
  pip install -r requirements.txt

Ejecutar:
  python app.py

Qué arregla v4 (importante para PROC ENF):
- Si un archivo tiene una hoja "resumen" y otra hoja "plantilla-compatible" (la que tiene TURNO + CODIGO),
  se procesa SOLO la hoja compatible y se salta la hoja resumen.
  Esto evita el desorden y los valores inválidos (ej. TURNO=L-M-V, fechas 01/01/1970, etc.).
- Se normaliza FECHA a dd/mm/yyyy.
- Se normaliza DNI (8 dígitos) para paciente y personal.
- TURNO se fuerza a M/T (otros valores quedan en blanco para no romper la plantilla).
- Ordena el resultado por fecha/turno/código.
- PROC. MED se llena solo con lo que cargues en "Proc Med"; PROC. ENF solo con "Proc Enf".
- Reporte TXT recomendado: muestra qué hojas entraron y cuáles se saltaron.

Si algo importante queda en SKIP:
- Esa hoja probablemente no tiene TURNO+CODIGO (no es formato plantilla) y requiere un parser dedicado.


v4.3: Arregla crash de sort (categorical/unordered) convirtiendo fechas a datetime robusto y ordenando por claves numéricas.


v4.3.3: Arregla carga lenta de excels con links externos (keep_links=False).


v4.3.4: Agrega opción para desactivar el ordenado (por si algún Excel trae fechas totalmente incompatibles).
