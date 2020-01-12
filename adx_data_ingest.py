from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder

AAD_TENANT_ID = "d55591ed-12fe-4a87-867c-081bd540f7cc"
KUSTO_URI = "https://hockey.southcentralus.kusto.windows.net:443/"
KUSTO_INGEST_URI = "https://ingest-hockey.southcentralus.kusto.windows.net:443/"
KUSTO_DATABASE = "hockey"

KCSB_INGEST = KustoConnectionStringBuilder.with_aad_device_authentication(
    KUSTO_INGEST_URI, AAD_TENANT_ID)

KCSB_DATA = KustoConnectionStringBuilder.with_aad_device_authentication(
    KUSTO_URI, AAD_TENANT_ID)

DESTINATION_TABLE = "players"

KUSTO_CLIENT = KustoClient(KCSB_DATA)
CREATE_TABLE_COMMAND = ".create table players (shortcut: string, first_name: string, last_name: string, active_flag: int, " \
                       "_src: string, begin_active: int, InjuriesIndirect: int, end_active: int)"


RESPONSE = KUSTO_CLIENT.execute_mgmt(KUSTO_DATABASE, CREATE_TABLE_COMMAND)

dataframe_from_result_table(RESPONSE.primary_results[0])
