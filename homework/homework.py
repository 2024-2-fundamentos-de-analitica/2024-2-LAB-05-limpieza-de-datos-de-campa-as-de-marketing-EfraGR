"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import pandas as pd
from zipfile import ZipFile

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    meses_dict = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    ruta_entrada = os.path.join("files", "input")
    ruta_salida = os.path.join("files", "output")
    archivos_comprimidos = [fichero for fichero in os.listdir(ruta_entrada) if fichero.endswith(".zip")]
    tabla_datos = []

    for fichero_zip in archivos_comprimidos:
        ruta_fichero_zip = os.path.join(ruta_entrada, fichero_zip)
        with ZipFile(ruta_fichero_zip) as archivo_zip:
            with archivo_zip.open(archivo_zip.namelist()[0]) as archivo_csv:
                datos_leidos = pd.read_csv(archivo_csv)
                if "Unnamed: 0" in datos_leidos.columns:
                    datos_leidos = datos_leidos.drop(columns=["Unnamed: 0"])
                tabla_datos.append(datos_leidos)

    datos_totales = pd.concat(tabla_datos, ignore_index=True)

    info_clientes = datos_totales[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    info_clientes["job"] = info_clientes["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    info_clientes["education"] = info_clientes["education"].replace("unknown", pd.NA).str.replace("-", "_", regex=False).str.replace(".", "_", regex=False)
    info_clientes["credit_default"] = info_clientes["credit_default"].apply(lambda valor: 1 if valor == "yes" else 0)
    info_clientes["mortgage"] = info_clientes["mortgage"].apply(lambda valor: 1 if valor == "yes" else 0)

    info_campania = datos_totales[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "month", "day"]]
    info_campania["month"] = info_campania["month"].apply(lambda valor: meses_dict.get(valor.lower(), "00"))
    info_campania["previous_outcome"] = info_campania["previous_outcome"].apply(lambda valor: 1 if valor == "success" else 0)
    info_campania["campaign_outcome"] = info_campania["campaign_outcome"].apply(lambda valor: 1 if valor == "yes" else 0)
    info_campania["last_contact_date"] = "2022-" + info_campania["month"].str.zfill(2) + "-" + info_campania["day"].astype(str).str.zfill(2)
    info_campania = info_campania.drop(columns=["month", "day"])

    datos_economicos = datos_totales[["client_id", "cons_price_idx", "euribor_three_months"]]

    if not os.path.exists(ruta_salida):
        os.makedirs(ruta_salida)

    info_clientes.to_csv(os.path.join(ruta_salida, "client.csv"), index=False)
    info_campania.to_csv(os.path.join(ruta_salida, "campaign.csv"), index=False)
    datos_economicos.to_csv(os.path.join(ruta_salida, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()