from __future__ import annotations

import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# In order to keep retailer names private, these are stored in a ':' delimited environment variable.
# Example: export RETAILERS="weasleys_wheezes:spacely_sprockets"
RETAILERS = os.environ.get("RETAILERS").split(':')

DISTANCE_THRESHOLD = 50
MAX_STORES = 50

INVENTORY_DIR = 'inventory'  # Only used for testing local file writes

s3_incoming_bucket_name = 'incoming'
s3_retailer_object_name = 'cvses.csv'
s3_output_bucket_name = 'private-inventory' # change this to 'inventory' when ready to be public


s3 = boto3.resource('s3',
                    endpoint_url=f"https://{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com",
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# S3 filesystem abstraction for reading parquet files
s3fs = pa.fs.S3FileSystem(access_key=AWS_ACCESS_KEY_ID, secret_key=AWS_SECRET_ACCESS_KEY, 
                          endpoint_override=f"https://{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com")


@dataclass
class UPCStock:
    upc: str
    level: str
    count: Optional[int]
    updated: str
    
    @classmethod
    def from_record(cls, record:dict) -> UPCStock:
        return cls(
            upc=record['Fmla_UPC'],
            level=record.get('InStock_Ind'),
            count=record.get('Nbr_UPC_Units'),
            updated=record['TimeStamp_Extract'],
        )

@dataclass
class Store:
    vendor: str
    code: str
    phone: Optional[str]
    web: Optional[str]
    addr: Optional[str]
    state: Optional[str]
    zip: Optional[str]  # I know, but trying to keep names short
    dist: float
    stock: list[UPCStock] = field(default_factory=list)
    
    @classmethod
    def from_record(cls, record:dict) -> Store:
        return cls(
            vendor=record['Retailer_Name'],
            code=record['Store_Code'],
            phone=record.get('Store_PhoneNbr'),
            web=record.get('Store_Website'),
            addr=record.get('Geo_Full_Address'),
            state=record.get('Geo_State_Abbrv'),
            zip=record.get('Geo_Zip_Code'),
            dist=record.get('distance'),
            stock=[])

    def add_stock(self, record: dict) -> None:
        self.stock.append(UPCStock.from_record(record))

def get_store_inventories(worldwide_inventory_df, store_id, distance):
    store_inventory_df = worldwide_inventory_df.loc[worldwide_inventory_df['Store_Code']==store_id, :]
    in_bounds_records = store_inventory_df.set_index(['Retailer_Name', 'Store_Code', 'Fmla_UPC'], 
                                                     drop=False).to_dict('records')
    stores = []
    current_code = None
    store = None
    for record in in_bounds_records:
        record['distance'] = distance
        store_code = f"{record['Retailer_Name']}-{record['Store_Code']}"
        if store_code != current_code:
            store = Store.from_record(record)
            stores.append(store)
            current_code = store_code
        store.add_stock(record)
    return stores

def write_local_file(filename, string):
    with open(INVENTORY_DIR / filename, "w") as filehandle:
        filehandle.write(content)

def write_remote_file(filename, content):
    output_object = s3.Object(s3_output_bucket_name, filename)
    result = output_object.put(Body=content)

for retailer in RETAILERS:
    s3_retailer_inventory_file = retailer + "_master.parquet"
    s3_retailer_local_stores_file = retailer + "_local_stores.parquet"

    retailer_inventory_table = pq.read_table(f"{s3_incoming_bucket_name}/{s3_retailer_inventory_file}", filesystem=s3fs)
    retailer_inventory_df = retailer_inventory_table.to_pandas()

    retailer_local_stores_table = pq.read_table(f"{s3_incoming_bucket_name}/{s3_retailer_local_stores_file}", filesystem=s3fs)
    retailer_local_stores_df = retailer_local_stores_table.to_pandas()

    for zip_code in retailer_local_stores_df.index:
        store_inventories = []
        for ordinal in range(MAX_STORES):
            store_id = retailer_local_stores_df.loc[zip_code, pd.IndexSlice[str(ordinal),'store_id']]
            distance = retailer_local_stores_df.loc[zip_code, pd.IndexSlice[str(ordinal),'distance']]
            if distance > DISTANCE_THRESHOLD:
                break
            store_inventories += get_store_inventories(retailer_inventory_df, store_id, distance)

        output_filename = f"{zip_code}_{DISTANCE_THRESHOLD}.json"
        content = json.dumps([asdict(store) for store in store_inventories], indent=2)
        write_remote_file(output_filename, content)
