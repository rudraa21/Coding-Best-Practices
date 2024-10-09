from notebooks_gold.utilities.application_specific_utilities import get_business_specific_filter_template_for_vmi_interface_and_retailer,extract_edi_msg_from_silver, get_edi_jda_mapping_file_data,mapping_edi_to_vmi_jda_mapping_file,ean_to_material_mapping
from tests_data import application_function_test_data as uf_test_data
from conftest import are_dataframes_equal 
import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import *


@pytest.mark.parametrize("test_name, interface, retailer, interface_data_filter_col, expected_output", uf_test_data.get_filter_template_test_cases)
def test_get_business_specific_filter_template(test_name, interface, retailer, interface_data_filter_col, expected_output):
    if expected_output == Exception:
        with pytest.raises(Exception):
            get_business_specific_filter_template_for_vmi_interface_and_retailer(interface, retailer, interface_data_filter_col)
    else:
        output = get_business_specific_filter_template_for_vmi_interface_and_retailer(interface, retailer, interface_data_filter_col)
        print(output)
        assert output == expected_output

@pytest.mark.parametrize("source_data,retailer_table_config,interface_name,retailer_name,expected_result",uf_test_data.get_extract_edi_msg_test_case)
def test_extract_edi_msg_from_silver(source_data,retailer_table_config,interface_name,retailer_name,expected_result,create_mock_dataframe,spark,mocker,monkeypatch):
    
    # mock return 
    def mock_return():
        return True

    # Create the mocked DataFrame
    mocked_data = create_mock_dataframe(source_data, ["name", "age", "BusinessDate"])
    mocked_df = create_mock_dataframe(expected_result, ["name"])
    monkeypatch.setattr("notebooks_gold.utilities.common_utilities.is_table_exists", mock_return)
    with mocker.patch("notebooks_gold.utilities.application_specific_utilities.get_dataframe", return_value=mocked_df):
    # # mocker.patch.object(spark, "table", return_value=mocked_data)
        result_df = extract_edi_msg_from_silver(
            spark=spark,
            retailer_table_config=retailer_table_config,
            interface_name=interface_name,
            retailer_name=retailer_name
        )
    expected_df = create_mock_dataframe(expected_result, ["name"])
    assert are_dataframes_equal(result_df, expected_df)

@pytest.mark.parametrize("config_data, expected_schema",uf_test_data.get_edi_jda_mapping_test_case)
def test_get_edi_jda_mapping_file_data(config_data, expected_schema,spark):
    # Call the function under test
    result_df = get_edi_jda_mapping_file_data(spark, config_data, expected_schema)

    # Perform assertions
    assert result_df.schema == expected_schema

@pytest.mark.parametrize("retailer_table_config,config_file_path,config_file_storage_type", uf_test_data.get_mapping_edi_to_vmi_jda_mapping_test_cases)
def test_mapping_edi_to_vmi_jda_mapping_file(retailer_table_config, config_file_path, config_file_storage_type,create_mock_dataframe, spark, mocker):
        mock_read_json={'data': [{'DMDGROUP': 'BE_VMI_OT_AHOLD_DELHAIZE', 'Description': 'DELHAIZE - IZ BROEKOOI Z4-210 FOOD', 'JDA_CODE': '01', 'JDA_LOC': 'FG_VMI_BE_ZEL_01', 'SOURCE_SYSTEM': '04DHF1', 'SOURCE_SYSTEM_COUNTRY': 'BE', 'SOURCE_UOM': 'CS', 'SUPPLIER_GLN': '5410048000019', 'WH_GLN': '5400110000122'}, {'DMDGROUP': 'BE_VMI_OT_AHOLD_DELHAIZE', 'Description': 'DELHAIZE NINOVE FOOD', 'JDA_CODE': '01', 'JDA_LOC': 'FG_VMI_BE_NIN_02', 'SOURCE_SYSTEM': '04DHF2', 'SOURCE_SYSTEM_COUNTRY': 'BE', 'SOURCE_UOM': 'CS', 'SUPPLIER_GLN': '5410048000019', 'WH_GLN': '5400110000306'}, {'DMDGROUP': 'BE_VMI_OT_AHOLD_DELHAIZE', 'Description': 'DELHAIZE NINOVE FAST MOVERS MANUAL FOOD', 'JDA_CODE': '01', 'JDA_LOC': 'FG_VMI_BE_NIN_01', 'SOURCE_SYSTEM': '04DHF3', 'SOURCE_SYSTEM_COUNTRY': 'BE', 'SOURCE_UOM': 'CS', 'SUPPLIER_GLN': '5410048000019', 'WH_GLN': '5400110000313'}, {'DMDGROUP': 'BE_VMI_OT_CRFGROUP_CARREFOUR', 'Description': 'Carrefour Kontich FOOD', 'JDA_CODE': '01', 'JDA_LOC': 'FG_VMI_BE_KON_01', 'SOURCE_SYSTEM': '04CFF1', 'SOURCE_SYSTEM_COUNTRY': 'BE', 'SOURCE_UOM': 'CS', 'SUPPLIER_GLN': '5410048000019', 'WH_GLN': '5400102000024'}, {'DMDGROUP': 'BE_VMI_OT_CRFGROUP_CARREFOUR', 'Description': 'Carrefour Puurs FOOD', 'JDA_CODE': '01', 'JDA_LOC': 'FG_VMI_BE_PUU_01', 'SOURCE_SYSTEM': '04CFF2', 'SOURCE_SYSTEM_COUNTRY': 'BE', 'SOURCE_UOM': 'CS', 'SUPPLIER_GLN': '5410048000019', 'WH_GLN': '5400102000048'}], 'parameters': {'param1': 'value1', 'param2': 'value2', 'param3': 'value3'}}
        mocker.patch("notebooks_gold.utilities.application_specific_utilities.read_json_metadata", return_value=mock_read_json)
        retailer_silver_df = create_mock_dataframe([(0,'5410048000019','5400102000048'),(1,'5410048000019','5400102000024')],["id","RetailWarehouseGlobalLocationNumber","SupplierGlobalLocationNumber"])
        result_df = mapping_edi_to_vmi_jda_mapping_file(
               spark=spark,
               retailer_silver_df=retailer_silver_df,
               retailer_table_config=retailer_table_config,
               config_file_path=config_file_path,
               config_file_storage_type=config_file_storage_type,
               env="dev"
        )
        expected_df=create_mock_dataframe([(0,'5410048000019','5400102000048','BE_VMI_OT_CRFGROUP_CARREFOUR','Carrefour Puurs FOOD','01','FG_VMI_BE_PUU_01','04CFF2','BE','CS','5410048000019','5400102000048'),(1,'5410048000019','5400102000024','BE_VMI_OT_CRFGROUP_CARREFOUR','Carrefour Kontich FOOD','01','FG_VMI_BE_KON_01','04CFF1','BE','CS','5410048000019','5400102000024')],["id","RetailWarehouseGlobalLocationNumber","SupplierGlobalLocationNumber","DMDGROUP","Description","JDA_CODE","JDA_LOC","SOURCE_SYSTEM","SOURCE_SYSTEM_COUNTRY","SOURCE_UOM","SUPPLIER_GLN","WH_GLN"])

    # Assertions on the result_df
        assert result_df.schema == expected_df.schema

@pytest.mark.parametrize("edi_jda_join_data, ean_material_mapping_data, expected_data",uf_test_data.get_ean_to_material_mapping_test_cases)
def test_ean_to_material_mapping(edi_jda_join_data, ean_material_mapping_data, expected_data,create_mock_dataframe, spark):
    edi_jda_join_df = create_mock_dataframe(edi_jda_join_data,["MaterialGlobalTradeItemNumber","Description"])
    ean_material_mapping_df = create_mock_dataframe(ean_material_mapping_data,["XTNUniversalProductCode","Country"])
    # Call the function under test
    result_df = ean_to_material_mapping(edi_jda_join_df, ean_material_mapping_df)
    expected_df = create_mock_dataframe(expected_data,["MaterialGlobalTradeItemNumber","Description","XTNUniversalProductCode","Country"])

    # Perform assertions
    assert are_dataframes_equal(result_df, expected_df)

################################









