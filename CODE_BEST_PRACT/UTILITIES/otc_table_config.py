from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from typing import List, Tuple, Optional, Dict, Any   
import os
import sys

# Initialize the ETL pipeline response dictionary
return_config_dict = {}



class OTCConfigManager:
    def __init__(self, spark: SparkSession, dbutils: Any):
        self.spark = spark
        self.dbutils = dbutils

    def define_silver_invoice_header_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                     system_id: str, instance_id: str, source_system_bu_grouping_list: List[str],business_unit_code:str
                                     ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice header config")
        table_config = {
            "table_name": "Invoice",
            "database_name": "fdn_sales_confidential_p2d",    #### handle p2d schema
            "watermark_column": "XTNRecordCreatedDate",
            "required_columns": ["InvoiceId", "XTNInvoiceCategoryIndicator", "XTNDocumentTypeName",
                                 "XTNRecordCreatedDate", "InvoiceDate", "XTNCanceledInvoiceDocumentIndicator","XTNSalesOrganizationCode"],
            "primary_key_columns": ["InvoiceId"],
            "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
            "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
        }
        instance_id = "P16"  #### REMOVE
        filter_template = [
            {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
            {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
            {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='},
            {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='},
            {"col_name": "XTNDocumentTypeName", "col_val": "'ZF2'", "operator": '=='},
            {"col_name": "XTNCanceledInvoiceDocumentIndicator", "operator": 'isNull'},
            {"col_name": "XTNSalesOrganizationCode", "col_val": f"{source_system_bu_grouping_list}", "operator": 'isin'}
        ]

        gold_otc_table_nm = "FactInvoice"
        gold_otc_db_nm = "otc_gold"

        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                )

        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
            filter_template += [
                {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},  ##change to watermark_column
                {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
            ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date
        table_config["load_tbl_name"] = gold_otc_table_nm
        table_config["load_db_name"] = gold_otc_db_nm

        return table_config

    def define_silver_invoice_item_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                    system_id: str, instance_id: str,business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "InvoiceLine",
            "database_name": "fdn_sales_confidential_p2d",
            "watermark_column": "XTNRecordCreatedDate",
            "required_columns": ["InvoiceId", "InvoiceLineNumber", "OrderId", "OrderLineNumber",
                                 "XTNRecordCreatedDate", "XTNDistributionChannelCode", "Quantity",
                                 "XTNNumeratorConversionOfSalesQuantity", "XTNBillingQuantityInStockKeepingUnitRate"],
            "primary_key_columns": ["InvoiceId", "InvoiceLineNumber"],
            "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
            "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
        }
        instance_id = "P16"  #### REMOVE
        filter_template = [
            {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
            {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
            {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='},
            {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='}
        ]

        gold_otc_table_nm = "FactInvoice"   ####need to be chnaged
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                )


        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
            filter_template += [
                {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
            ]
        
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type

        return table_config


    def define_silver_order_header_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                     system_id: str, instance_id: str, source_system_bu_grouping_list: List[str],business_unit_code:str
                                     ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
            "table_name": "Order",
            "database_name": "fdn_sales_confidential_p2d",
            "watermark_column": "XTNRecordCreatedDate",
            "required_columns" : ["OrderId", "OrderRequestedDeliveryDate", "XTNRecordCreatedDate", 
                                  "XTNSalesDocumentCategoryCode", "DistributionChannelId", "CustomerId",
                                  "XTNSalesOrganizationCode", "XTNDivisionCode","XTNDFSystemId","XTNDFReportingUnitId",
                                  "XTNSalesDocumentTypeCode","XTNBusinessTransactionOrderReasonCode", "XTNSalesGroupCode", 
                                  "XTNShippingConditionCode"
                                  ],
            "primary_key_columns": ["OrderId"],
            "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
            "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
        }
        instance_id = "P16"  #### REMOVE
        filter_template = [
            {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
            {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
            {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='},
            {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='},
            {"col_name": "XTNSalesOrganizationCode", "col_val": f"{source_system_bu_grouping_list}", "operator": 'isin'}
        ]

        gold_otc_table_nm = "FactOrder"   ####need to be chnaged
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                )
        
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
            filter_template += [
                {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
            ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date
        table_config["load_tbl_name"] = gold_otc_table_nm
        table_config["load_db_name"] = gold_otc_db_nm

        return table_config

    def define_silver_order_item_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                    system_id: str, instance_id: str,business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order item config")
        table_config = {
            "table_name": "XTNOrderLine",
            "database_name": "fdn_sales_confidential_p2d",
            "watermark_column": "OrderCreatedDate",
            "required_columns": ["SalesDocumentNumber","OrderItemLine","MaterialId","SalesByEachQuantity",
                                 "XTNSalesUnitCumulativeOrderQuantity","RejectionReasonCode","ItemCategoryCode","DivisionId",
                                 "ReferenceDocumentId","ReferenceItemNumber","SalesUnitOfMeasure","XTNLocationId",
                                 "XTNBusinessTransactionOrderReasonCode","SalesOrganizationCode","OrderCreatedDate","DenominatorForSalesQuantityToSKU",
                                 "NumeratorForSalesQuantityToSKU","OrderRouteId"
                                ],
            "primary_key_columns": ["SalesDocumentNumber","OrderItemLine"],
            "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
            "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
        }
        instance_id = "P16"  #### REMOVE
        filter_template = [
            {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
            {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
            {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='},
            {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='}
        ]

        gold_otc_table_nm = "FactOrder"   ####need to be chnaged
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                )
        

        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
            filter_template += [
                {"col_name": "OrderCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                {"col_name": "OrderCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
            ]
        
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type

        return table_config
                    


    def define_silver_delivery_header_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                        system_id: str, instance_id: str, source_system_bu_grouping_list: List[str],business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
                "table_name": "FulfillmentPlan",
                "database_name": "fdn_sales_confidential_p2d",
                "watermark_column": "XTNRecordCreatedDate",
                "required_columns" : ["FulfillmentPlanId","XTNRecordCreatedDate","XTNTransportRouteId","XTNActualGoodsMovementDate", "XTNSalesOrganizationCode"],
                "primary_key_columns": ["FulfillmentPlanId"],
                "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
                "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
            }
        instance_id = "P16"  #### REMOVE
        filter_template = [
                {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
                {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
                {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='},
                {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='},
                {"col_name": "XTNSalesOrganizationCode", "col_val": f"{source_system_bu_grouping_list}", "operator": 'isin'}
            ]

        gold_otc_table_nm = "factdelivery"   ####need to be chnaged
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                    table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                    )
            
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
                filter_template += [
                    {"col_name":  table_config["watermark_column"], "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                    {"col_name":  table_config["watermark_column"], "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
                ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date

        return table_config


    
    def define_silver_delivery_line_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                        system_id: str, instance_id: str, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
                "table_name": "XTNFulfillmentPlanLine",
                "database_name": "fdn_sales_confidential_p2d",
                "watermark_column": "RecordCreatedDate",
                "required_columns" : ["FulfillmentPlanId","FulfilmentItemNumber","ReferenceDocumentId","LocationId","ReferenceItemNumber","RecordCreatedDate",
                                                                    "ActualDeliveredSalesUnitQuantity", "ActualDeliveredStockKeepingUnitQuantity","FulfilmentNetWeightQuantity",
                                                                    "WeightUnitCode","NumeratorConversionOfSalesQuantity","DivisionCode","DistributionChannelCode",
                                                                    "OrderLineTypeIndicator","MaterialId","DeliveryItemCategoryCode"],
                "primary_key_columns": ["FulfillmentPlanId","FulfilmentItemNumber"],
                "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
                "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
            }
        instance_id = "P16"  #### REMOVE
        filter_template = [
                {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
                {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
                {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='},
                {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='}
                
                # {"col_name": "XTNSalesOrganizationCode", "col_val": f"{source_system_bu_grouping_list}", "operator": 'isin'} ask rajeev if we have any sales or col in iutem levekl
            ]

        gold_otc_table_nm = "factdelivery"   ####need to be chnaged
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                    table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                    )
            
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
                filter_template += [
                    {"col_name": table_config["watermark_column"], "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                    {"col_name": table_config["watermark_column"], "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
                ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type

        return table_config
    


    def define_gold_delivery_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
                "table_name": "factdelivery",
                "database_name": "otc_gold",
                "watermark_column": "XTNUpdatedTime",
                "required_columns" : [],
                "primary_key_columns": ["FulfillmentPlanId","FulfilmentItemNumber"]
            }
        filter_template = [ {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='}]

        gold_otc_table_nm = "finalordertocash"   
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                    table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                    )
            
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
                filter_template += [
                    {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                    {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
                ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date

        return table_config
    

    def define_gold_order_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
                "table_name": "factorder",
                "database_name": "otc_gold",
                "watermark_column": "OrderCreatedDate",
                "required_columns" : [],
                "primary_key_columns": ["OrderId", "OrderItemLine"]
            }
        filter_template =  [ {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='}]
        gold_otc_table_nm = "finalordertocash"   
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                    table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                    )
            
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
                filter_template += [
                    {"col_name": "OrderCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                    {"col_name": "OrderCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
                ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date

        return table_config
    
    def define_gold_refusal_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
                "table_name": "FactRefusal",
                "database_name": "otc_gold",
                "watermark_column": "RefusalOrderCreatedDate",
                "required_columns" : [],
                "primary_key_columns": ["RefusalOrderId", "RefusalOrderItemLine"]
            }
        filter_template =  [ {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='}]
        gold_otc_table_nm = "finalordertocash"   
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                    table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                    )
            
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":  #### checl with rajeeb on order created or refusal order cre
                filter_template += [
                    {"col_name": table_config["watermark_column"], "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                    {"col_name": table_config["watermark_column"], "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
                ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date

        return table_config


    def define_gold_invoice_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining order header config")
        table_config = {
                "table_name": "FactInvoice",
                "database_name": "otc_gold",
                "watermark_column": "XTNUpdatedTime",
                "required_columns" : [],
                "primary_key_columns": ["FulfillmentPlanId","FulfilmentItemNumber"]
            }
        filter_template =  [ {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='}]
        gold_otc_table_nm = "finalordertocash"   ####need to be chnaged
        gold_otc_db_nm = "otc_gold"
        three_month_data_filter_start_date, three_month_data_filter_end_date, load_type = self.identify_business_date_filter_window(
                                                                                                                                    table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
                                                                                                                                    )
            
        if three_month_data_filter_start_date and three_month_data_filter_end_date and load_type.lower() == "incremental":
                filter_template += [
                    {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_start_date}'", "operator": ">=", "conditional_operator": "&"},
                    {"col_name": "XTNRecordCreatedDate", "col_val": f"'{three_month_data_filter_end_date}'", "operator": "<=", "conditional_operator": "&"}
                ]
        table_config["filter_template"] = filter_template
        table_config["load_type"] = load_type
        table_config["cut_off_filter_date"] = three_month_data_filter_start_date

        return table_config





   


    # def identify_business_date_filter_window(self, table_name: str, database_name: str, business_unit_code: str) -> Tuple[Optional[str], Optional[str], str]:
    #     #config_logger.info("Identifying business date filter window")
    #     try:
    #         target_tbl = f"{database_name}.{table_name}"
            
    #         if self.spark and target_tbl and not self.spark.table(target_tbl).isEmpty():  ###### filter market filter
    #             current_date_str = datetime.now().strftime("%Y-%m-%d")

    #             gold_source_system_col_name = 'ReportingBusinessUnitCode'
    #             xtn_update_time = 'XTNUpdatedTime'

    #             #target_tbl = "otc_gold.salescustomer"  ### REMOVE

    #             #config_logger.info(f"Querying table {target_tbl} for latest update time")
    #             source_system_based_last_updated_date = self.spark.table(target_tbl) \
    #                 .select(gold_source_system_col_name, xtn_update_time) \
    #                 .filter(f"{gold_source_system_col_name} = '{business_unit_code}'") \
    #                 .agg({xtn_update_time: 'max'}).collect()[0][0]

    #             #config_logger.info(f"Latest update time: {source_system_based_last_updated_date}")
    #             if source_system_based_last_updated_date :
    #                 if not isinstance(source_system_based_last_updated_date, datetime):
    #                     source_system_based_last_updated_date = datetime.strptime(source_system_based_last_updated_date, "%Y-%m-%d")
                    
    #                 ninety_days_back_date = (source_system_based_last_updated_date - timedelta(days=90)).strftime("%Y-%m-%d")

    #                 three_month_filter_start_date = ninety_days_back_date
    #                 three_month_filter_end_date = current_date_str
    #                 load_type = "incremental"
    #             else:
    #                 #config_logger.warning(f"Table {target_tbl} is empty or does not exist, performing full load")
    #                 three_month_filter_start_date = None
    #                 three_month_filter_end_date = None
    #                 load_type = "full_load"
    #         else:
    #             #config_logger.warning(f"Table {target_tbl} is empty or does not exist, performing full load")
    #             three_month_filter_start_date = None
    #             three_month_filter_end_date = None
    #             load_type = "full_load"

    #         return three_month_filter_start_date, three_month_filter_end_date, load_type
    #     except Exception as e:
    #         error_message = f"Error in identifying business date filter window for table {table_name}: {e}"
    #         #config_logger.error(error_message)
    #         raise Exception(error_message)

    
    # # 
    #                                         "table_name": "deliverydelaytemplate",
    #                                         "database_name": "otc_gold",
    #                                         "watermark_column": "",
    #                                         "load_type": load_type,
    #                                         "required_delay_delivery_columns" : ["DeliveryNumberDelayedId","DelayOrOnTimeDescription","DelayReasonsDescription"]
    # #                                     },
    

    def define_dim_silver_route_table_config(self,instance_id: str, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                    system_id: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "xtnroutetransportationlogistic",
            "database_name": "fdn_masterdata_internal_p2d",
            "required_columns": ["RouteId","TransitGoodsIssueToDeliveryCalendarDayQuantity"],
            "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
            "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
        }
        #instance_id = "Q16"  #### REMOVE   chnage imp
        filter_template = [
            {"col_name": "XTNDFSystemId", "col_val": f"'{xtn_df_system_id}'", "operator": '=='},
            {"col_name": "XTNDFReportingUnitId", "col_val": f"'{xtn_df_reporting_unit_id}'", "operator": '=='},
            {"col_name": "SystemId", "col_val": f"'{system_id}'", "operator": '=='}
            ,
            {"col_name": "InstanceId", "col_val": f"'{instance_id}'", "operator": '=='},   ##chck imp
        ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_delay_table_config(self, xtn_df_system_id: str, xtn_df_reporting_unit_id: str,
                                    system_id: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "deliverydelaytemplate",
            "database_name": "otc_gold",
            "required_columns": ["DeliveryNumberDelayedId","DelayOrOnTimeDescription","DelayReasonsDescription"],
            "system_id": {"col_name": "XTNDFSystemId", "value_id": xtn_df_system_id},
            "reporting_id": {"col_name": "XTNDFReportingUnitId", "value_id": xtn_df_reporting_unit_id}
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": "'EUOTC'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_sales_cust_table_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "salescustomer",
            "database_name": "otc_gold",
            "required_columns": ["CustomerKey", "CustomerId", "SalesOrganizationCode", "DistributionChannelId", "DivisionCode"]
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_location_table_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "location",
            "database_name": "otc_gold",
            "required_columns": ["LocationKey","LocationId"]
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_material_table_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "material",
            "database_name": "otc_gold",
            "required_columns": ["MaterialKey","MaterialId","MaterialTypeCode"]
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_ord_reason_table_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "salesdocumentorderreasondescription",
            "database_name": "otc_gold",
            "required_columns": ["SalesDocumentOrderReasonKey","SalesDocumentOrderReasonCode"]
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_rej_reason_table_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "salesdocumentrejectionreasondescription",
            "database_name": "otc_gold",
            "required_columns": ["SalesDocumentRejectionReasonKey","SalesDocumentRejectionReasonCode"]
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config
    
    def define_dim_gold_sales_org_table_config(self, business_unit_code:str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info("Defining invoice item config")
        table_config = {
            "table_name": "salesorganization",
            "database_name": "otc_gold",
            "required_columns": ["SalesOrganizationKey","SalesOrganizationId"]
        }
        
        filter_template = [
                            {"col_name": "ReportingBusinessUnitCode", "col_val": f"'{business_unit_code}'", "operator": '=='},
                         ]
        
        table_config["filter_template"] = filter_template

        return table_config

    def identify_business_date_filter_window(self, table_name: str, database_name: str, business_unit_code: str) -> Tuple[Optional[str], Optional[str], str]:
        """
        Identifies the business date filter window based on the most recent update in the target table.

        Args:
            table_name (str): Name of the table to query.
            database_name (str): Name of the database containing the table.
            business_unit_code (str): Business unit code to filter the data.

        Returns:
            Tuple[Optional[str], Optional[str], str]: Start date, end date, and load type.
        """
        try:
            target_tbl = f"{database_name}.{table_name}"

            if self.spark and target_tbl:
                df = self.spark.table(target_tbl)

                if not df.isEmpty():
                    current_date_str = datetime.now().strftime("%Y-%m-%d")
                    gold_source_system_col_name = 'ReportingBusinessUnitCode'
                    xtn_update_time = 'XTNUpdatedTime'

                    source_system_based_last_updated_date = df.select(gold_source_system_col_name, xtn_update_time) \
                        .filter(f"{gold_source_system_col_name} = '{business_unit_code}'") \
                        .agg({xtn_update_time: 'max'}).collect()[0][0]

                    if source_system_based_last_updated_date:
                        if not isinstance(source_system_based_last_updated_date, datetime):
                            source_system_based_last_updated_date = datetime.strptime(source_system_based_last_updated_date, "%Y-%m-%d")

                        ninety_days_back_date = (source_system_based_last_updated_date - timedelta(days=90)).strftime("%Y-%m-%d")
                        return ninety_days_back_date, current_date_str, "incremental"

            # If the table is empty or doesn't exist, return full load.
            return None, None, "full_load"

        except ValueError as ve:
            error_message = f"Date parsing error in identifying business date filter window for table {table_name}: {ve}"
            raise Exception(error_message)
        except Exception as e:
            error_message = f"Error in identifying business date filter window for table {table_name}: {e}"
            raise Exception(error_message)

    


    def get_table_config(
                        self, 
                        table_name: str, 
                        database_name: str, 
                        business_unit_code: str,
                        xtn_df_system_id: Optional[str] = None, 
                        xtn_df_reporting_unit_id: Optional[str] = None,
                        system_id: Optional[str] = None, 
                        instance_id: Optional[str] = None, 
                        source_system_bu_grouping_list: Optional[List[str]] = None
                        ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        #config_logger.info(f"Getting config for table {table_name} in database {database_name}")
        # gold_otc_table_nm = "finalordertocash"
        # gold_otc_db_nm = "otc_gold"

        # three_month_filter_start_date, three_month_filter_end_date, load_type = self.identify_business_date_filter_window(
        #                                                                                                                         table_name=gold_otc_table_nm, database_name=gold_otc_db_nm, business_unit_code=business_unit_code
        #                                                                                                                         )

        if table_name.lower() == "invoice":
            return self.define_silver_invoice_header_config(

                xtn_df_system_id=xtn_df_system_id,
                xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                system_id=system_id,
                instance_id=instance_id,
                source_system_bu_grouping_list=source_system_bu_grouping_list,
                business_unit_code=business_unit_code
            )
        elif table_name.lower() == "invoiceline":
            return self.define_silver_invoice_item_config(
                xtn_df_system_id=xtn_df_system_id,
                xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                system_id=system_id,
                instance_id=instance_id,
                business_unit_code=business_unit_code
            )
        elif table_name.lower() == "order":
            return self.define_silver_order_header_config(
                xtn_df_system_id=xtn_df_system_id,
                xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                system_id=system_id,
                instance_id=instance_id,
                source_system_bu_grouping_list=source_system_bu_grouping_list,
                business_unit_code=business_unit_code
            )
        elif table_name.lower() == "xtnorderline":
            return self.define_silver_order_item_config(
                xtn_df_system_id=xtn_df_system_id,
                xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                system_id=system_id,
                instance_id=instance_id,
                business_unit_code=business_unit_code
            )
        elif table_name.lower() == "xtnroutetransportationlogistic":
            return self.define_dim_silver_route_table_config(
                                                                xtn_df_system_id=xtn_df_system_id,
                                                                instance_id =instance_id,
                                                                xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                                                                system_id=system_id
                                                             ) 
        elif table_name.lower() == "deliverydelaytemplate":
            return self.define_dim_gold_delay_table_config(
                                                                xtn_df_system_id=xtn_df_system_id,
                                                                xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                                                                system_id=system_id
                                                             )
             
        elif table_name.lower() == "fulfillmentplan":
            return self.define_silver_delivery_header_config(
                                                        xtn_df_system_id=xtn_df_system_id,
                                                        xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                                                        system_id=system_id,
                                                        instance_id=instance_id,
                                                        source_system_bu_grouping_list=source_system_bu_grouping_list,
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "xtnfulfillmentplanline":
            return self.define_silver_delivery_line_config(
                                                        xtn_df_system_id=xtn_df_system_id,
                                                        xtn_df_reporting_unit_id=xtn_df_reporting_unit_id,
                                                        system_id=system_id,
                                                        instance_id=instance_id,
                                                        #source_system_bu_grouping_list=source_system_bu_grouping_list,
                                                        business_unit_code=business_unit_code
            )

        elif table_name.lower() == "salescustomer":
            return self.define_dim_gold_sales_cust_table_config(
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "material":
            return self.define_dim_gold_material_table_config(
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "location":
            return self.define_dim_gold_location_table_config(
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "salesdocumentorderreasondescription":
            return self.define_dim_gold_ord_reason_table_config(
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "salesdocumentrejectionreasondescription":
            return self.define_dim_gold_rej_reason_table_config(
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "salesorganization":
            return self.define_dim_gold_sales_org_table_config(
                                                        business_unit_code=business_unit_code
                                                    )
        elif table_name.lower() == "factdelivery":
            return self.define_gold_delivery_config(
                                                           business_unit_code=business_unit_code
                                                          )
        elif table_name.lower() == "factorder":
            return self.define_gold_order_config(
                                                           business_unit_code=business_unit_code
                                                          )
        elif table_name.lower() == "factrefusal":
            return self.define_gold_refusal_config(
                                                           business_unit_code=business_unit_code
                                                          )
        elif table_name.lower() == "factinvoice":
            return self.define_gold_invoice_config(
                                                           business_unit_code=business_unit_code
                                                          )
        else:
            raise ValueError(f"Unknown table name: {table_name}")
