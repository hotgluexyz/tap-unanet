"""Stream type classes for tap-unanet."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_unanet.client import UnanetStream


class GeneralLedgerStream(UnanetStream):
    """Define custom stream."""
    name = "general_ledger"
    table_name = "general_ledger"
    primary_keys = ["general_ledger_key"]
    # replication_key = "transaction_date"
    replication_key = "post_date"
    
    schema = th.PropertiesList(
        th.Property("general_ledger_key", th.NumberType),
        th.Property("feature", th.NumberType),
        th.Property("feature", th.NumberType),
        th.Property("post_date", th.DateTimeType),
        th.Property("fiscal_month_key", th.NumberType),
        th.Property("account_key", th.NumberType),
        th.Property("organization_key", th.NumberType),
        th.Property("document_number", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("description", th.StringType),
        th.Property("transaction_date", th.DateTimeType),
        th.Property("quantity", th.NumberType),
        th.Property("debit_amount", th.NumberType),
        th.Property("credit_amount", th.NumberType),
        th.Property("project_key", th.NumberType),
        th.Property("person_key", th.NumberType),
        th.Property("customer_key", th.NumberType),
        th.Property("local_debit_amount", th.NumberType),
        th.Property("local_credit_amount", th.NumberType),
        th.Property("instance_debit_amount", th.NumberType),
        th.Property("instance_credit_amount", th.NumberType),
        th.Property("transaction_currency", th.NumberType),
        th.Property("local_currency", th.NumberType),
        
        
    ).to_dict()


class AccountsStream(UnanetStream):
    name = "accounts"
    primary_keys = ["account_key"]
    table_name = "account"
    replication_key = None
    schema = th.PropertiesList(
        th.Property("account_key", th.IntegerType),
        th.Property("account_code", th.StringType),
        th.Property("description", th.StringType),
        th.Property("type", th.StringType),
        th.Property("active", th.StringType),
        th.Property("entry_allowed", th.StringType),
        th.Property("begin_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("project_required", th.StringType),
        th.Property("hide_income_stmt_hdr", th.StringType),
        th.Property("category_1099", th.StringType),
    ).to_dict()
class CustomersStream(UnanetStream):
    name = "customers"
    primary_keys = ["customer_key"]
    table_name = "customer"
    replication_key = "last_updated_timestamp"
    schema = th.PropertiesList(
        th.Property("customer_key", th.IntegerType),
        th.Property("customer_code", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_type_key", th.IntegerType),
        th.Property("customer_size", th.IntegerType),
        th.Property("account_number", th.StringType),
        th.Property("sic_code", th.StringType),
        th.Property("classification", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("stock_symbol", th.StringType),
        th.Property("url", th.StringType),
        th.Property("active", th.StringType),
        th.Property("financial_org", th.StringType),
        th.Property("legal_entity", th.StringType),
        th.Property("legal_entity_key", th.IntegerType),
        th.Property("default_gl_post_org_key", th.IntegerType),
        th.Property("entry_allowed", th.StringType),
        th.Property("begin_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("entry_allowed", th.StringType),
        th.Property("recipient_name_1099", th.StringType),
        th.Property("vendor_1099", th.StringType),
        th.Property("federal_tax_id", th.StringType),
        th.Property("federal_tax_id_type", th.IntegerType),
        th.Property("email_1099", th.StringType),
        th.Property("last_project_code_seq", th.IntegerType),
        th.Property("start_with_project_code_seq", th.IntegerType),
        th.Property("transact_elimination_flag", th.StringType),
        th.Property("last_updated_timestamp", th.DateTimeType),
        th.Property("created_timestamp", th.DateTimeType),
        th.Property("currency_code_key", th.IntegerType),
        th.Property("default_person_org_flag", th.StringType),
    ).to_dict()
class PersonsStream(UnanetStream):
    name = "persons "
    primary_keys = ["person_key"]
    table_name = "person"
    replication_key = "last_modified_timestamp"
    schema = th.PropertiesList(
        th.Property("person_key", th.IntegerType),
        th.Property("person_code", th.StringType),
        th.Property("username", th.StringType),
        th.Property("password_date", th.StringType),
        th.Property("ssn", th.StringType),
        th.Property("emp_id", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("middle_initial", th.StringType),
        th.Property("suffix", th.StringType),
        th.Property("email", th.StringType),
        th.Property("customer_key", th.IntegerType),
        th.Property("pay_code_key", th.IntegerType),
        th.Property("hour_increment", th.NumberType),
        th.Property("time_period_key", th.NumberType),
        th.Property("expense_vendor_key", th.IntegerType),
        th.Property("active", th.StringType),
        th.Property("timesheet_lines", th.StringType),
        th.Property("timesheet_email", th.StringType),
        th.Property("leave_request_email", th.StringType),
        th.Property("expense_email", th.StringType),
        th.Property("hide_vat", th.StringType),
        th.Property("tito_required", th.StringType),
        th.Property("payment_method_key", th.IntegerType),
        th.Property("currency_key", th.IntegerType),
        th.Property("expense_approval_amount", th.NumberType),
        th.Property("vehicle_number", th.StringType),
        th.Property("project_key", th.IntegerType),
        th.Property("task_key", th.IntegerType),
        th.Property("labor_category_key", th.IntegerType),
        th.Property("business_week_key", th.IntegerType),
        th.Property("assignment_email", th.CustomType({"type": ["number", "string"]})),
        th.Property("hire_date", th.DateTimeType),
        th.Property("payment_currency_key", th.IntegerType),
        th.Property("vendor_invoice_person", th.StringType),
        th.Property("po_form_title", th.StringType),
        th.Property("po_signature_key", th.IntegerType),
        th.Property("subcontractor", th.StringType),
        th.Property("activation_pending", th.StringType),
        th.Property("ar_approval_amount", th.NumberType),
        th.Property("customer_invoice_email", th.StringType),
        th.Property("subk_user_account_activated", th.StringType),
        th.Property("integration_user", th.StringType),
        th.Property("created_timestamp", th.DateTimeType),
        th.Property("last_modified_timestamp", th.DateTimeType),
        th.Property("email_verified", th.StringType),
        th.Property("currency_code_key", th.IntegerType),
        th.Property("default_legal_entity_key", th.IntegerType),
    ).to_dict()
