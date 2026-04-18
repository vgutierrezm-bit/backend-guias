from fastapi import FastAPI, UploadFile
from fastapi.responses import StreamingResponse
import pandas as pd
import pdfplumber
import re
import zipfile
from io import BytesIO

app = FastAPI()

# Estructura en memoria:
# {
#   "numero_orden": ["guia1", "guia2", ...]
# }
orden_a_guias = {}


# =====================================================
# ENDPOINT 1: PROCESAR PDFs
# =====================================================
@app.post("/procesar_pdfs")
async def procesar_pdfs(pdfs: list[UploadFile]):
    orden_a_guias.clear()
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for pdf in pdfs:
            texto = ""

            with pdfplumber.open(pdf.file) as pdf_file:
                for page in pdf_file.pages:
                    contenido = page.extract_text()
                    if contenido:
                        texto += contenido

            # Guía: 10 a 12 dígitos
            guia_match = re.search(r"\b\d{10,12}\b", texto)
            # Orden: 9 a 11 dígitos
            orden_match = re.search(r"\b\d{9,11}\b", texto)

            if not guia_match or not orden_match:
                continue

            guia = guia_match.group()
            orden = orden_match.group()

            if orden not in orden_a_guias:
                orden_a_guias[orden] = []
            orden_a_guias[orden].append(guia)

            pdf.file.seek(0)
            zipf.writestr(f"{orden}_{guia}.pdf", pdf.file.read())

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": "attachment; filename=pdfs_renombrados.zip"
        }
    )


# =====================================================
# ENDPOINT 2: PROCESAR EXCEL
# =====================================================
@app.post("/procesar_excel")
async def procesar_excel(excel: UploadFile):
    # Lee la hoja OFFLINE
    df = pd.read_excel(excel.file, sheet_name="OFFLINE")

    filas_finales = []

    for _, fila in df.iterrows():
        # Columna D (índice 3) = número de orden
        orden = str(fila.iloc[3]).strip()

        if orden in orden_a_guias:
            # Duplica la fila por cada guía
            for guia in orden_a_guias[orden]:
                nueva_fila = fila.copy()
                nueva_fila["Guia transportadora"] = guia
                filas_finales.append(nueva_fila)
        else:
            filas_finales.append(fila)

    df_final = pd.DataFrame(filas_finales)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_final.to_excel(writer, index=False, sheet_name="OFFLINE")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=OFFLINE_COMPLETO.xlsx"
        }
    )
