import os
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout,
    QPushButton, QLabel, QFileDialog, QMessageBox, QCheckBox
)
from engine import consolidate


class App(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Consolidador Hospital - Excel a Plantilla")
        self.resize(760, 380)

        self.plantilla = None
        self.bd_pac = None
        self.bd_per = None
        self.proc_med = []
        self.proc_enf = []
        self.out_path = None

        layout = QVBoxLayout()

        self.lbl = QLabel("Carga archivos y presiona PROCESAR.")
        layout.addWidget(self.lbl)

        btn_tpl = QPushButton("Cargar Plantilla.xlsx")
        btn_tpl.clicked.connect(self.pick_tpl)
        layout.addWidget(btn_tpl)

        btn_bd1 = QPushButton("Cargar BD Pacientes.xlsx")
        btn_bd1.clicked.connect(self.pick_bd_pac)
        layout.addWidget(btn_bd1)

        btn_bd2 = QPushButton("Cargar BD Personal.xlsx")
        btn_bd2.clicked.connect(self.pick_bd_per)
        layout.addWidget(btn_bd2)

        btn_med = QPushButton("Cargar Excels Proc Med (multi)")
        btn_med.clicked.connect(self.pick_med)
        layout.addWidget(btn_med)

        btn_enf = QPushButton("Cargar Excels Proc Enf (multi)")
        btn_enf.clicked.connect(self.pick_enf)
        layout.addWidget(btn_enf)

        btn_out = QPushButton("Elegir ruta de salida .xlsx")
        btn_out.clicked.connect(self.pick_out)
        layout.addWidget(btn_out)

        self.chk_audit = QCheckBox("Generar hoja AUDITORIA (opcional)")
        self.chk_audit.setChecked(False)
        layout.addWidget(self.chk_audit)

        self.chk_report = QCheckBox("Guardar reporte .txt (recomendado)")
        self.chk_report.setChecked(True)
        layout.addWidget(self.chk_report)

        btn_run = QPushButton("PROCESAR")
        btn_run.clicked.connect(self.run)
        layout.addWidget(btn_run)

        self.setLayout(layout)

    def pick_tpl(self):
        f, _ = QFileDialog.getOpenFileName(self, "Plantilla", "", "Excel (*.xlsx)")
        if f:
            self.plantilla = f
            self.lbl.setText(f"Plantilla: {os.path.basename(f)}")

    def pick_bd_pac(self):
        f, _ = QFileDialog.getOpenFileName(self, "BD Pacientes", "", "Excel (*.xlsx)")
        if f:
            self.bd_pac = f
            self.lbl.setText(f"BD Pacientes: {os.path.basename(f)}")

    def pick_bd_per(self):
        f, _ = QFileDialog.getOpenFileName(self, "BD Personal", "", "Excel (*.xlsx)")
        if f:
            self.bd_per = f
            self.lbl.setText(f"BD Personal: {os.path.basename(f)}")

    def pick_med(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Proc Med", "", "Excel (*.xlsx)")
        if files:
            self.proc_med = files
            self.lbl.setText(f"Proc Med: {len(files)} archivos")

    def pick_enf(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Proc Enf", "", "Excel (*.xlsx)")
        if files:
            self.proc_enf = files
            self.lbl.setText(f"Proc Enf: {len(files)} archivos")

    def pick_out(self):
        f, _ = QFileDialog.getSaveFileName(self, "Salida", "Resultado.xlsx", "Excel (*.xlsx)")
        if f:
            self.out_path = f
            self.lbl.setText(f"Salida: {os.path.basename(f)}")

    def run(self):
        if not all([self.plantilla, self.bd_pac, self.bd_per, self.out_path]):
            QMessageBox.warning(self, "Falta algo", "Carga plantilla, BD pacientes, BD personal y salida.")
            return
        if not self.proc_med and not self.proc_enf:
            QMessageBox.warning(self, "Falta algo", "Carga al menos archivos de Proc Med o Proc Enf.")
            return

        try:
            consolidate(
                plantilla_xlsx=self.plantilla,
                pacientes_xlsx=self.bd_pac,
                personal_xlsx=self.bd_per,
                proc_med_files=self.proc_med,
                proc_enf_files=self.proc_enf,
                out_xlsx=self.out_path,
                include_audit_sheet=self.chk_audit.isChecked(),
                write_report_txt=self.chk_report.isChecked(),
            )
            extra = ""
            if self.chk_report.isChecked():
                base, _ = os.path.splitext(self.out_path)
                extra = f"\nReporte: {base}_REPORTE.txt"
            QMessageBox.information(self, "Listo", f"Generado: {self.out_path}{extra}")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))


if __name__ == "__main__":
    app = QApplication([])
    w = App()
    w.show()
    app.exec()
