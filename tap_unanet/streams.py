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
    

class PnLDetailStream(UnanetStream):
    name = "pnl_detail"
    table_name = "general_ledger"
    primary_keys = ["gl_key"]
    replication_key = "post_date"
    order_by_key = "gl.post_date"
    schema = th.PropertiesList(
        th.Property("gl_key", th.IntegerType),
        th.Property("feature", th.NumberType),
        th.Property("post_date", th.DateTimeType),
        th.Property("fiscal_month_key", th.IntegerType),
        th.Property("account_key", th.IntegerType),
        th.Property("organization_key", th.IntegerType),
        th.Property("document_number", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("description", th.StringType),
        th.Property("transaction_date", th.DateTimeType),
        th.Property("quantity", th.NumberType),
        th.Property("debit_amount", th.NumberType),
        th.Property("credit_amount", th.NumberType),
        th.Property("project_key", th.IntegerType),
        th.Property("project_name", th.StringType),
        th.Property("person_key", th.IntegerType),
        th.Property("customer_key", th.IntegerType),
        th.Property("local_debit_amount", th.NumberType),
        th.Property("local_credit_amount", th.NumberType),
        th.Property("instance_debit_amount", th.NumberType),
        th.Property("instance_credit_amount", th.NumberType),
        th.Property("transaction_currency", th.NumberType),
        th.Property("local_currency", th.NumberType),
        th.Property("account_code", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("account_key", th.IntegerType),
        th.Property("account_type", th.StringType),
        th.Property("customer_code", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_type_key", th.IntegerType),
        th.Property("customer_type", th.StringType),
        th.Property("organization_code", th.StringType),
        th.Property("organization_name", th.StringType),
        th.Property("organization_type_key", th.IntegerType),
        th.Property("organization_type", th.StringType),
        th.Property("person_code", th.StringType),
        th.Property("person_first_name", th.StringType),
        th.Property("person_last_name", th.StringType),
        th.Property("net_amount", th.NumberType),
    ).to_dict()
    
    @property
    def query(self):
        return f"SELECT gl.general_ledger_key as gl_key, gl.feature,gl.post_date,gl.fiscal_month_key,gl.account_key,gl.organization_key,gl.document_number,gl.reference,gl.description,gl.transaction_date,gl.quantity,gl.debit_amount,gl.credit_amount,gl.project_key,gl.person_key,gl.customer_key,gl.local_debit_amount,gl.local_credit_amount,gl.instance_debit_amount,gl.instance_credit_amount,gl.transaction_currency,gl.local_currency,a.account_code,a.account_key,a.type as account_type,a.description as account_name,c.customer_code as organization_code, c_.customer_code as customer_code,c.customer_name as organization_name, c_.customer_name as customer_name,c.customer_type_key as organization_type_key,c_.customer_type_key as customer_type_key,org_ct.customer_type as organization_type,ct.customer_type as customer_type,p.person_code,p.first_name as person_first_name,p.last_name as person_last_name,pr.title as project_name FROM {self.schema_name}.general_ledger gl LEFT JOIN {self.schema_name}.account a ON gl.account_key = a.account_key LEFT JOIN {self.schema_name}.customer c ON gl.organization_key = c.customer_key LEFT JOIN {self.schema_name}.customer c_ ON gl.customer_key = c_.customer_key LEFT JOIN {self.schema_name}.customer_type org_ct ON c.customer_type_key = org_ct.customer_type_key LEFT JOIN {self.schema_name}.customer_type ct ON c_.customer_type_key = ct.customer_type_key LEFT JOIN {self.schema_name}.person p ON gl.person_key = p.person_key LEFT JOIN {self.schema_name}.project pr ON gl.project_key = pr.project_key "
    # customer can represent many entities, here both customer_key and organization_key make reference to the same table
    # that's why there are 2 joins on the same table ticket: HGI-6156
    
    @property
    def query_total(self):
        return f"SELECT COUNT(*) AS total FROM {self.schema_name}.general_ledger gl LEFT JOIN {self.schema_name}.account a ON gl.account_key = a.account_key"
    
    def post_process(self, row, context):
        try:
            # Ignore selected catalog map all properties
            # properties_list = self.schema['properties'].keys()
            properties_list = [
                "gl_key","feature","post_date","fiscal_month_key","account_key","organization_key","document_number","reference","description","transaction_date","quantity","debit_amount","credit_amount","project_key","person_key","customer_key","local_debit_amount","local_credit_amount","instance_debit_amount","instance_credit_amount","transaction_currency","local_currency","account_code","account_key","account_type","account_name","organization_code","customer_code","organization_name","customer_name","organization_type_key","customer_type_key","organization_type","customer_type","person_code","person_first_name","person_last_name","project_name"
            ]
            combined_dict = dict(zip(properties_list, row))
            if combined_dict.get("account_type") in ["R", "E"]:
                # Calculate net amount
                self.logger.info("Calculating totals for net amount...")
                if combined_dict.get("account_type") == "R":
                    combined_dict["net_amount"] = combined_dict.get("credit_amount") - combined_dict.get("debit_amount")
                elif combined_dict.get("account_type") == "E":
                    combined_dict["net_amount"] = combined_dict.get("debit_amount") - combined_dict.get("credit_amount")
                self.logger.info(f"Processed pnl row {combined_dict}")
                return combined_dict
        except Exception as e:
            self.logger.error(f"Error in post_process: {e}")
            print(f"Error in post_process: {row}")
            return None


class ProjectsStream(UnanetStream):
    """Define custom stream."""
    name = "projects"
    table_name = "project"
    primary_keys = ["project_key"]
    
    schema = th.PropertiesList(
        th.Property("project_key", th.NumberType),
        th.Property("customer_key", th.NumberType),
        th.Property("owning_customer_key", th.NumberType),
        th.Property("project_code", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("title", th.StringType),
        th.Property("purpose", th.StringType),
        th.Property("pay_code_key", th.NumberType),
        th.Property("project_type_key", th.NumberType),
        th.Property("project_status_key", th.NumberType),
        th.Property("billing_type_key", th.NumberType),
        th.Property("posting_group_key", th.NumberType),
        th.Property("cost_struct_key", th.NumberType),
        th.Property("orig_start_date", th.DateTimeType),
        th.Property("orig_end_date", th.DateTimeType),
        th.Property("rev_start_date", th.DateTimeType),
        th.Property("rev_end_date", th.DateTimeType),
        th.Property("completed_date", th.DateTimeType),
        th.Property("total_value", th.NumberType),
        th.Property("funded_value", th.NumberType),
        th.Property("hours_budget", th.NumberType),
        th.Property("hours_etc", th.NumberType),
        th.Property("hours_est_tot", th.NumberType),
        th.Property("exp_bill_budget", th.NumberType),
        th.Property("exp_cost_budget", th.NumberType),
        th.Property("exp_cost_burden_budget", th.NumberType),
        th.Property("exp_bill_etc", th.NumberType),
        th.Property("exp_cost_etc", th.NumberType),
        th.Property("exp_bill_est_tot", th.NumberType),
        th.Property("exp_cost_est_tot", th.NumberType),
        th.Property("labor_bill_budget", th.NumberType),
        th.Property("labor_cost_budget", th.NumberType),
        th.Property("labor_cost_burden_budget", th.NumberType),
        th.Property("labor_bill_etc", th.NumberType),
        th.Property("labor_cost_etc", th.NumberType),
        th.Property("labor_bill_est_tot", th.NumberType),
        th.Property("labor_cost_est_tot", th.NumberType),
        th.Property("assignment_flag", th.StringType),
        th.Property("expense_assignment_flag", th.StringType),
        th.Property("time_assignment_flag", th.StringType),
        th.Property("task_level_assignment", th.StringType),
        th.Property("allow_self_plan", th.StringType),
        th.Property("self_assign_plans", th.StringType),
        th.Property("er_task_required", th.StringType),
        th.Property("ts_task_required", th.StringType),
        th.Property("future_charge", th.StringType),
        th.Property("tito_required", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("probability_percent", th.NumberType),
        th.Property("percent_complete", th.NumberType),
        th.Property("bill_rate_source", th.StringType),
        th.Property("cost_rate_source", th.StringType),
        th.Property("use_labor_category", th.StringType),
        th.Property("enforce_wbs_dates", th.StringType),
        th.Property("leave_balance", th.StringType),
        th.Property("customer_approves_first", th.StringType),
        th.Property("pct_complete_rule", th.StringType),
        th.Property("project_color", th.StringType),
        th.Property("proj_require_time_comments", th.StringType),
        th.Property("location_required", th.StringType),
        th.Property("location_key", th.NumberType),
        th.Property("limit_bill_to_funded", th.StringType),
        th.Property("limit_rev_to_funded", th.StringType),
        th.Property("user01", th.StringType),
        th.Property("user02", th.StringType),
        th.Property("user03", th.StringType),
        th.Property("user04", th.StringType),
        th.Property("user05", th.StringType),
        th.Property("user06", th.StringType),
        th.Property("user07", th.StringType),
        th.Property("user08", th.StringType),
        th.Property("user09", th.StringType),
        th.Property("user10", th.StringType),
        th.Property("ts_sub_po_required", th.StringType),
        th.Property("exp_sub_po_required", th.StringType),
        th.Property("item_assignment_flag", th.StringType),
        th.Property("item_task_required", th.StringType),
        th.Property("pm_approves_before_mgr", th.StringType),
        th.Property("user11", th.StringType),
        th.Property("user12", th.StringType),
        th.Property("user13", th.StringType),
        th.Property("user14", th.StringType),
        th.Property("user15", th.StringType),
        th.Property("user16", th.StringType),
        th.Property("user17", th.StringType),
        th.Property("user18", th.StringType),
        th.Property("user19", th.StringType),
        th.Property("user20", th.StringType),
    ).to_dict()


class AccountHierarchyStream(UnanetStream):
    """Define custom stream."""
    name = "account_hierarchy"
    table_name = "acct_fin_tree"
    primary_keys = ["node_key"]
    
    schema = th.PropertiesList(
        th.Property("node_key", th.NumberType),
        th.Property("parent_key", th.NumberType),
        th.Property("tree_level", th.IntegerType),
        th.Property("left_visit", th.IntegerType),
        th.Property("right_visit", th.IntegerType),
    ).to_dict()