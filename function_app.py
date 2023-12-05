import json
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger función que procesa peticiones GET.')

    storage_account_name = "eloymistorageaccount01"

    if "storage_account_key" in os.environ:  # Se podría simplificar.
        storage_account_key = os.environ.get("storage_account_key")
    else:
        raise Exception(
            "No se ha proporcionado el storage account key en la ejecución. Si la ejecución es local, hay que actualizarlo en local.setting.json")

    container_name_raw = "raw"
    container_name_processed = "processed"

    files = {
        "sales": {"csv": "sales.csv", 
                  "parquet": "sales.parquet"}, 
        "products": {"csv": "products.csv", 
                    "parquet": "products.parquet"},
        "prices": {"csv": "prices.csv", 
                   "parquet": "prices.parquet"}
    }

    for key in files: 
        blob_client_raw = init_blob_client_session(storage_account_name=storage_account_name,
                                                storage_account_key=storage_account_key,
                                                container_name=container_name_raw,
                                                blob_name=files[key]["csv"])

        df_products = read_csv_files_storage(blob_client=blob_client_raw)

        blob_client_processed = init_blob_client_session(storage_account_name=storage_account_name,
                                                        storage_account_key=storage_account_key,
                                                        container_name=container_name_processed,
                                                        blob_name=files[key]["parquet"])

        write_parquet_to_blob(df_p=df_products, blob_client=blob_client_processed)

    return func.HttpResponse("", status_code=200)


def init_blob_client_session(storage_account_name, storage_account_key, container_name, blob_name):
    """
    Inicialización del blob_client para tener acceso a los ficheros.
    """
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    return blob_client


def read_csv_files_storage(blob_client):
    """
    Función que recibe una cuenta de storage, un acoount key del storage, un contenedor y el nombre de un fichero, y realiza la lectura del fichero
    y devuelve un dataframe de pandas con el resultado
    """

    blob_data = blob_client.download_blob()
    csv_data = blob_data.readall()

    df_p = pd.read_csv(BytesIO(csv_data), sep=';')

    return df_p


def read_csv_files_storage_sas_token():
    """"
    En esta función, vamos a ver la lectura de ficheros a través de las sas url.

    Las limitaciones de este método es la capacidad de hacer lecturas y escrituras en el contenedor.
    """
    # Conexión al servicio de Blob Storage
    sas_url = "NUESTRA_SAS_URL"

    # Leyendo Dataframe
    df_p = pd.read_csv(sas_url, sep=";")

    return df_p


def write_parquet_to_blob(df_p, blob_client):
    """
    Función que recibe una sesión de blob_client, un dataframe y guarda en la sesión recibida el dataframe.
    """

    # Convert DataFrame to Parquet in-memory
    table = pa.Table.from_pandas(df_p)
    parquet_data = BytesIO()
    pq.write_table(table, parquet_data)

    # Upload Parquet to Blob Storage
    blob_client.upload_blob(parquet_data.getvalue(), overwrite=True)