from base64 import encode
import pandas as pd
import requests
import base64
import json
import ast

cred = {
    {
    "client_id": "YOUR CLIENT ID",
    "client_secret": "YOUR CLIENT SECRET"
}
}
def get_bearer_token() -> dict:
    encoded_cred = base64.b64encode(
                    bytes(
                        cred["client_id"]+":"+cred["client_secret"],
                        "utf-8"
                )).decode("utf-8")
    auth_options = {
        "url": "https://accounts.spotify.com/api/token",
        "headers": {
            "Authorization": "Basic {}".format(encoded_cred)
        },
        "data": {
        "grant_type": "client_credentials"
        }
    }

    token_info = ast.literal_eval(requests.post(
        url=auth_options["url"],
        headers=auth_options["headers"],
        data=auth_options["data"],
        ).content.decode("utf-8"))

    return token_info

def catch_and_count_data() -> int:
    token_info = get_bearer_token(open(cred_file))
    artist_id = "6XyY86QOPPrYVGvF9ch6wz"
    req_options = { "url": "https://api.spotify.com/v1/artists/{}/albums".format(artist_id),
                    "headers": {
                        "Authorization": "{} {}".format(token_info["token_type"], token_info["access_token"]),
                }}
    albums = requests.get(
        url=req_options["url"],
        headers=req_options["headers"]
        ).json()
    
    albums_df = pd.DataFrame(data=albums["items"])

    return albums_df["name"].count()

    
def is_valid(ti):
    qty = ti.xcom_pull(task_ids="catch_and_count_data")
    if qty > 10:
        return "valid"
    else:
        return "invalid"
