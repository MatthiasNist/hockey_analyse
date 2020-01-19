"""A simple example how to use KustoClient."""

from datetime import timedelta
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from pymongo import MongoClient
from hockey_analysis import GetData

"""Samples on how to use Kusto Ingest client. Just Replace variables and run!"""

from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    KustoIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    StreamDescriptor,
    DataFormat,
    ReportLevel,
    IngestionMappingType,
    KustoStreamingIngestClient,
)


######################################################
##                        AUTH                      ##
######################################################

# Note that the 'help' cluster only allows interactive
# access by AAD users (and *not* AAD applications)

# Erstellen der Tabelle 'players' auf dem Cluster https://hockey.southcentralus.kusto.windows.net ueber advice-auth (GUI)

cluster = "https://hockey.southcentralus.kusto.windows.net"
kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)
client_create = KustoClient(kcsb)
DESTINATION_TABLE = "players"
CREATE_TABLE_COMMAND = ".create table players (shortcut: string, first_name: string, last_name: string, active_flag: int, " \
                       "_src: string, begin_active: int, end_active: int)"
KUSTO_DATABASE = "hockey"

RESPONSE = client_create.execute_mgmt(KUSTO_DATABASE, CREATE_TABLE_COMMAND)
# Code auf Terminal

dataframe_from_result_table(RESPONSE.primary_results[0]) # Players noch leer

# NEXT STEP: Datenerfassung, ingest der Daten in die Tabelle playersd
# https://docs.microsoft.com/de-de/azure/kusto/management/data-ingestion/

# ueber advice-auth (GUI): -> LINK 1
KUSTO_INGEST_URI = "https://ingest-hockey.southcentralus.kusto.windows.net:443/"
kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(KUSTO_INGEST_URI)

# In case you want to authenticate with AAD application.
# client_id = "<insert here your AAD application id>"
# client_secret = "<insert here your AAD application key>"
# authority_id = "<insert here your tenant id>"


client = KustoIngestClient(kcsb)
ingestion_props = IngestionProperties(
    database="hockey",
    table="players",
    dataFormat=DataFormat.CSV,
    # in case status update for success are also required
    # reportLevel=ReportLevel.FailuresAndSuccesses,
    # in case a mapping is required
    ingestionMappingReference="{"
    # ingestionMappingType=IngestionMappingType.Json
)

import pandas as pd

mongo_client = MongoClient('mongodb://localhost:27017/')
# mongo_client.database_names()
db = mongo_client.get_database("hockey")
players_col = db.get_collection("players")
cur_players = players_col.find({})
df_players = pd.DataFrame(cur_players)
df_players.drop("_id", axis = 1, inplace = True)

client.ingest_from_dataframe(df_players, ingestion_properties=ingestion_props)

query = "players | take 10"

response = client_create.execute(KUSTO_DATABASE, query)
for row in response.primary_results[0]:
    print(row)

# Mapping: https://docs.microsoft.com/de-de/azure/kusto/management/mappings
# oder mapping bei query...?

