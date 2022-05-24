from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Dict, Optional

import boto3
import geopandas
import pandas as pd
from geojson import Point as GeoPoint
from geopandas import points_from_xy
from shapely.geometry.point import Point

CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

METERS_TO_MILES = 1609
PROJECTION = 'EPSG:3857'  # https://epsg.io/3857
RADIUS_IN_MI = 2
ZIP_CODE = '20004'


s3_incoming_bucket_name = 'incoming'
s3_retailer_object_name = 'cvses.csv'
s3_output_bucket_name = 'inventory'


s3 = boto3.resource('s3',
                    endpoint_url=f"https://{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com",
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


# Output data structures as Python data classes

@dataclass
class UPCStock:
    upc: str
    level: str
    count: Optional[int]
    updated: str

    @classmethod
    def from_record(cls, record: dict) -> UPCStock:
        return cls(
            upc=record['Fmla_UPC'],
            level=record.get('InStock_Ind'),
            count=record.get('Nbr_UPC_Units'),
            updated=record['Timestamp_Extract'],
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
    geometry: dict
    stock: list[UPCStock] = field(default_factory=list)

    @classmethod
    def from_record(cls, record: dict) -> Store:
        return cls(
            vendor=record['vendor'],
            code=record['Store_Code'],
            phone=record.get('Store_PhoneNbr'),
            web=record.get('Store_Website'),
            addr=record.get('Geo_Full_Address'),
            state=record.get('Geo_StateAbbrv'),
            zip=record.get('Geo_Zip_Code'),
            dist=record.get('distance_from_center'),
            geometry=dict(GeoPoint(record.get('geometry').coords[0])),
            stock=[])

    def add_stock(self, record: dict) -> None:
        self.stock.append(UPCStock.from_record(record))


# Sample retail input data #1
retailer_remote_object = s3.Object(
    s3_incoming_bucket_name, s3_retailer_object_name)
retailer_response = retailer_remote_object.get()
retailer_frame = pd.read_csv(retailer_response['Body'], dtype='string')
retailer_frame['vendor'] = 'cvs'

retailer = geopandas.GeoDataFrame(
    retailer_frame,
    geometry=points_from_xy(
        x=retailer_frame.Geo_Longitude,
        y=retailer_frame.Geo_Latitude,
        crs='EPSG:4326',
    ))

# Grabbed from HUD's GIS holdings at https://hudgis-hud.opendata.arcgis.com/datasets/d032efff520b4bf0aa620a54a477c70e/explore?showTable=true
# Note that these leave out empty zips, which for our purposes seems fine?
zip_centroids = geopandas.read_file(
    'ZIP_Code_Population_Weighted_Centroids.geojson')
zip_centroids.set_index('STD_ZIP5', drop=False, inplace=True)


def get_distance(frame: geopandas.GeoDataFrame, zips: geopandas.GeoDataFrame, zipcode: str) -> geopandas.GeoDataFrame:
    frame = frame.to_crs(PROJECTION)
    zips = zips.to_crs(PROJECTION)
    center = zips.loc[zipcode]
    return frame.geometry.distance(center.geometry)


retailer['distance_from_center'] = get_distance(
    retailer, zip_centroids, ZIP_CODE)/METERS_TO_MILES

in_bounds_retailer = retailer[retailer.distance_from_center < RADIUS_IN_MI]
in_bounds_retailer.shape

in_bounds_records = in_bounds_retailer.set_index(
    ['vendor', 'Store_Code', 'Fmla_UPC'], drop=False).to_dict('records')

stores = []
current_code = None
store = None
for record in in_bounds_records:
    store_code = f"{record['vendor']}-{record['Store_Code']}"
    if store_code != current_code:
        store = Store.from_record(record)
        stores.append(store)
        current_code = store_code
    store.add_stock(record)

inventory_json_string = json.dumps(
    [asdict(store) for store in stores], indent=2)

output_filename = f"{ZIP_CODE}_{RADIUS_IN_MI}.json"
output_object = s3.Object(s3_output_bucket_name, output_filename)

result = output_object.put(Body=inventory_json_string)

print(result)