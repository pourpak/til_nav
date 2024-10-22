from r2connect.r2client import R2Client
import r2connect.exceptions.cloudflare.r2
import os
import constants

os.environ["ENDPOINT_URL"] = constants.cloudflare["ENDPOINT_URL"]
os.environ["ACCESS_KEY"] = constants.cloudflare["ACCESS_KEY"]
os.environ["SECRET_KEY"] = constants.cloudflare["ENDPOINT_URL"]
os.environ["REGION"] = "weur"

bucket_name = "master-thesis"

# nrk_raw, aftenposten_raw, norgesbank_raw, nettavisen_raw, vg_raw
download_file_path = "data/nettavisen_raw.parquet"
object_name = "nettavisen_raw.parquet"

r2_client = R2Client()

try:
    r2_client.download_file(object_name, bucket_name, download_file_path)
except r2connect.exceptions.cloudflare.r2.BucketDoesNotExist:
    print(f"The specified bucket does not exist: {bucket_name}")
except r2connect.exceptions.cloudflare.r2.ObjectAlreadyExists:
    print(f"An object with the same object_key already exists: {object_name}")
except Exception as error:
    print(error)
