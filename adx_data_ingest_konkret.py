"""A simple example how to use KustoClient."""

from datetime import timedelta
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table


######################################################
##                        AUTH                      ##
######################################################

# Note that the 'help' cluster only allows interactive
# access by AAD users (and *not* AAD applications)

# Erstellen der Tabelle 'players' auf dem Cluster https://hockey.southcentralus.kusto.windows.net ueber advice-auth (GUI)

cluster = "https://hockey.southcentralus.kusto.windows.net"
kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)
client = KustoClient(kcsb)
DESTINATION_TABLE = "players"
CREATE_TABLE_COMMAND = ".create table players (shortcut: string, first_name: string, last_name: string, active_flag: int, " \
                       "_src: string, begin_active: int, end_active: int)"
KUSTO_DATABASE = "hockey"

RESPONSE = client.execute_mgmt(KUSTO_DATABASE, CREATE_TABLE_COMMAND)

dataframe_from_result_table(RESPONSE.primary_results[0]) # Players noch leer

# NEXT STEP: Datenerfassung, ingest der Daten in die Tabelle playersd
# https://docs.microsoft.com/de-de/azure/kusto/management/data-ingestion/

KUSTO_INGEST_URI = "https://ingest-hockey.southcentralus.kusto.windows.net:443/"