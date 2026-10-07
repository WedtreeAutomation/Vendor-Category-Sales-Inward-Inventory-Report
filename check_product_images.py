import io
import streamlit as st
from PIL import Image
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

st.set_page_config(page_title="OneLake Image Test")

# Secrets
TENANT_ID = st.secrets["TENANT_ID"]
CLIENT_ID = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]

ONELAKE_ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com"


def get_client():
    credential = ClientSecretCredential(
        TENANT_ID,
        CLIENT_ID,
        CLIENT_SECRET,
    )

    return DataLakeServiceClient(
        account_url=ONELAKE_ACCOUNT_URL,
        credential=credential,
    )


def parse_abfss_path(abfss_path):
    parsed = urlparse(abfss_path)

    filesystem = parsed.username
    file_path = parsed.path.lstrip("/")

    return filesystem, file_path


def fetch_image(abfss_path):
    client = get_client()

    filesystem, file_path = parse_abfss_path(abfss_path)

    st.write("Filesystem:", filesystem)
    st.write("File Path:", file_path)

    fs_client = client.get_file_system_client(filesystem)

    file_client = fs_client.get_file_client(file_path)

    downloader = file_client.download_file()

    image_bytes = downloader.readall()

    st.write("Downloaded Bytes:", len(image_bytes))

    img = Image.open(io.BytesIO(image_bytes))

    return img


st.title("OneLake Image Test")

sample_path = st.text_area(
    "ABFSS Path",
    value="abfss://xxxxxxxx@onelake.dfs.fabric.microsoft.com/xxxxx/Files/Odoo_Images/1713531.png",
    height=120,
)

if st.button("Load Image"):
    try:
        img = fetch_image(sample_path)

        st.success("Image Loaded Successfully")

        st.image(img)

    except Exception as e:
        st.error(f"{type(e).__name__}: {e}")