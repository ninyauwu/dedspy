import pandas as pd
from settings import Settings, logger
import sqlite3

settings = Settings()
logger.add("logfile.log")

def select_from(name, connection):
    dataframe = pd.read_sql_query("SELECT * FROM " + name, connection)

    columni = {}
    for column in dataframe.columns:
        if "TRIAL" in column:
            dataframe.drop(column, axis=1, inplace=True)

        columni[column] = name.upper() + "_" + column.lower()
    dataframe.rename(columns=columni, inplace=True)
    return dataframe


# Automatically creates a surrogate column from the first column in the dataframe, under the assumption it is the
# primary key
def select_from_with_surrogate(name, connection):
    dataframe = pd.read_sql_query("SELECT * FROM " + name, connection)
    dataframe[dataframe.columns[0] + '_surrogate'] = range(len(dataframe))

    columni = {}
    for column in dataframe.columns:
        if "TRIAL" in column:
            dataframe.drop(column, axis=1, inplace=True)

        columni[column] = name.upper() + "_" + column.lower()
    dataframe.rename(columns=columni, inplace=True)
    return dataframe


def find_mutual_columns(dataset1, dataset2):
    column_names = set([])
    mutual_column_names = []
    for column in dataset1:
        column_names.add(column)
    for column in dataset2:
        if column in column_names:
            mutual_column_names.append(column)
    return mutual_column_names


def merge_differing_columns_simple(dataframe1, dataframe2, key):
    # Pak de unieke columns uit de tweede dataframe
    column_names1 = set(dataframe1.columns)
    column_names2 = set(dataframe2.columns)
    uniques_from_2 = [key]
    for column in column_names2:
        if column not in column_names1:
            uniques_from_2.append(column)

    # Merge deze met de eerste dataframe
    return pd.merge(dataframe1, dataframe2.loc[:, uniques_from_2], on=key, how='left')


def merge_differing_columns(dataframe1, dataframe2, left_key, right_key):
    # Pak de unieke columns uit de tweede dataframe
    column_names1 = set(dataframe1.columns)
    column_names2 = set(dataframe2.columns)
    uniques_from_2 = [right_key]
    for column in column_names2:
        if column not in column_names1:
            uniques_from_2.append(column)

    # Merge deze met de eerste dataframe
    return pd.merge(dataframe1, dataframe2.loc[:, uniques_from_2], left_on=left_key, right_on=right_key, how='left')


def send_to_ssms(dataframe, target_name, cursor, conn):
    cursor.execute("DELETE FROM " + target_name)
    conn.commit()
    column_string = "("
    value_string = "("
    i = 0
    for column in dataframe.columns:
        column_string += column
        value_string += "?"
        if i == dataframe.columns.size - 1:
            column_string += ")"
            value_string += ")"
        else:
            column_string += ", "
            value_string += ", "
        i += 1

    for index, row in dataframe.iterrows():
        values = []
        for column in dataframe.columns:
            values.append(str(row[column]))
        query = "INSERT INTO " + target_name + " " + column_string + " VALUES " + value_string + ";"
        cursor.execute(query, values)
    conn.commit()
    # print(value_string)


def send_to_ssms_upper(dataframe, target_name, cursor, conn):
    cursor.execute("DELETE FROM " + target_name)
    conn.commit()
    column_string = "("
    value_string = "("
    i = 0
    for column in dataframe.columns:
        column_string += column
        value_string += "?"
        if i == dataframe.columns.size - 1:
            column_string += ")"
            value_string += ")"
        else:
            column_string += ", "
            value_string += ", "
        i += 1

    for index, row in dataframe.iterrows():
        values = []
        for column in dataframe.columns:
            values.append(str(row[column]))
        query = "INSERT INTO " + target_name + " " + column_string.upper() + " VALUES " + value_string + ";"
        cursor.execute(query, values)
    conn.commit()


def importdb():
    # Sales
    connectie_sales = sqlite3.connect('data/raw/go_sales.sqlite')
    #sql_query = "SELECT name FROM sqlite_master WHERE type='table';"

    product = select_from_with_surrogate("product", connectie_sales)
    product_type = select_from("product_type", connectie_sales)
    product_line = select_from("product_line", connectie_sales)
    SALES_sales_staff = select_from_with_surrogate("sales_staff", connectie_sales)
    SALES_sales_branch = select_from("sales_branch", connectie_sales)
    SALES_retailer_site = select_from("retailer_site", connectie_sales)
    SALES_country = select_from("country", connectie_sales)
    order_header = select_from("order_header", connectie_sales)
    order_details = select_from("order_details", connectie_sales)
    order_method = select_from_with_surrogate("order_method", connectie_sales)
    target = select_from("SALES_TARGETData", connectie_sales)
    returned_item = select_from("returned_item", connectie_sales)
    return_reason = select_from_with_surrogate("return_reason", connectie_sales)
    sales_target = select_from("SALES_TARGETData", connectie_sales)

    # Staff
    connectie_staff = sqlite3.connect('data/raw/go_staff.sqlite')

    course = select_from_with_surrogate("course", connectie_staff)
    STAFF_sales_staff = select_from("sales_staff", connectie_staff)
    STAFF_sales_branch = select_from("sales_branch", connectie_staff)
    satisfaction = select_from("satisfaction", connectie_staff)
    satisfaction_type = select_from_with_surrogate("satisfaction_type", connectie_staff)
    training = select_from("training", connectie_staff)

    # CRM
    connectie_crm = sqlite3.connect('data/raw/go_crm.sqlite')

    age_group = select_from("age_group", connectie_crm)
    CRM_country = select_from("country", connectie_crm)
    retailer = select_from("retailer", connectie_crm)
    retailer_contact = select_from("retailer_contact", connectie_crm)
    retailer_headquarters = select_from("retailer_headquarters", connectie_crm)
    retailer_segment = select_from("retailer_segment", connectie_crm)
    CRM_retailer_site = select_from_with_surrogate("retailer_site", connectie_crm)
    retailer_type = select_from("retailer_type", connectie_crm)
    sales_demographic = select_from("sales_demographic", connectie_crm)
    sales_territory = select_from("sales_territory", connectie_crm)

    # CSV files
    inventory_levels = pd.read_csv('data/raw/GO_SALES_INVENTORY_LEVELSData.csv')
    product_forecast = pd.read_csv('data/raw/GO_SALES_PRODUCT_FORECASTData.csv')

    # Merge similar tables
    sales_staff = merge_differing_columns_simple(STAFF_sales_staff, SALES_sales_staff, 'SALES_STAFF_sales_staff_code')
    sales_branch = merge_differing_columns_simple(SALES_sales_branch, STAFF_sales_branch,
                                                  'SALES_BRANCH_sales_branch_code')
    SALES_country.rename({'COUNTRY_country': 'COUNTRY_country_en'}, axis=1, inplace=True)
    country = merge_differing_columns_simple(SALES_country, CRM_country, 'COUNTRY_country_code')
    retailer_site = merge_differing_columns_simple(SALES_retailer_site, CRM_retailer_site,
                                                   'RETAILER_SITE_retailer_site_code')

    # PRODUCT
    output_PRODUCT = pd.merge(product, product_type, left_on='PRODUCT_product_type_code',
                              right_on='PRODUCT_TYPE_product_type_code', how='left')
    output_PRODUCT.drop('PRODUCT_TYPE_product_type_code', axis=1, inplace=True)
    output_PRODUCT = pd.merge(output_PRODUCT, product_line, left_on='PRODUCT_TYPE_product_line_code',
                              right_on='PRODUCT_LINE_product_line_code', how='left')
    output_PRODUCT.drop('PRODUCT_LINE_product_line_code', axis=1, inplace=True)
    send_to_ssms(output_PRODUCT, 'PRODUCT', Settings.export_cursor, Settings.export_conn)

    # RETURN_REASON
    output_RETURN_REASON = return_reason
    send_to_ssms(output_RETURN_REASON, 'RETURN_REASON', Settings.export_cursor, Settings.export_conn)

    # SALES_TARGETData
    output_SALES_TARGETData = sales_target
    send_to_ssms(output_SALES_TARGETData, 'SALES_TARGETData', Settings.export_cursor, Settings.export_conn)

    # PRODUCT_FORECASTData
    output_PRODUCT_FORECASTData = product_forecast
    output_PRODUCT_FORECASTData.rename(
        {'PRODUCT_NUMBER': 'PRODUCT_FORECASTData_product_number', 'YEAR': 'PRODUCT_FORECASTData_year',
         'MONTH': 'PRODUCT_FORECASTData_month', 'EXPECTED_VOLUME': 'PRODUCT_FORECASTData_expected_volume'}, axis=1,
        inplace=True)
    send_to_ssms(output_PRODUCT_FORECASTData, 'PRODUCT_FORECASTData', Settings.export_cursor, Settings.export_conn)

    # INVENTORY_LEVELSData
    output_INVENTORY_LEVELSData = inventory_levels
    output_INVENTORY_LEVELSData.rename({'INVENTORY_YEAR': 'INVENTORY_LEVELSData_inventory_year',
                                        'INVENTORY_MONTH': 'INVENTORY_LEVELSData_inventory_month',
                                        'PRODUCT_NUMBER': 'INVENTORY_LEVELSData_product_number',
                                        'INVENTORY_COUNT': 'INVENTORY_LEVELSData_inventory_count'}, axis=1,
                                       inplace=True)
    send_to_ssms(output_INVENTORY_LEVELSData, 'INVENTORY_LEVELSData', Settings.export_cursor, Settings.export_conn)

    # SALES_STAFF
    output_SALES_STAFF = pd.merge(sales_staff, sales_branch, left_on='SALES_STAFF_sales_branch_code',
                                  right_on='SALES_BRANCH_sales_branch_code')
    output_SALES_STAFF.drop('SALES_BRANCH_sales_branch_code', axis=1, inplace=True)
    output_SALES_STAFF = pd.merge(output_SALES_STAFF, country, left_on='SALES_BRANCH_country_code',
                                  right_on='COUNTRY_country_code')
    output_SALES_STAFF.drop('COUNTRY_country_code', axis=1, inplace=True)
    send_to_ssms(output_SALES_STAFF, 'SALES_STAFF', Settings.export_cursor, Settings.export_conn)

    # ORDER_DETAILS
    output_ORDER_DETAILS = pd.merge(order_details, order_header, left_on='ORDER_DETAILS_order_number',
                                    right_on='ORDER_HEADER_order_number')
    output_ORDER_DETAILS.drop('ORDER_HEADER_order_number', axis=1, inplace=True)
    send_to_ssms(output_ORDER_DETAILS, 'ORDER_DETAILS', Settings.export_cursor, Settings.export_conn)

    # RETURNED_ITEM
    output_RETURNED_ITEM = returned_item
    send_to_ssms_upper(output_RETURNED_ITEM, 'RETURNED_ITEM', Settings.export_cursor, Settings.export_conn)

    # ORDER METHOD
    output_ORDER_METHOD = order_method
    send_to_ssms(output_ORDER_METHOD, 'ORDER_METHOD', Settings.export_cursor, Settings.export_conn)

    # RETAILER_SITE
    output_RETAILER_SITE = pd.merge(retailer, retailer_type, left_on='RETAILER_retailer_type_code',
                                    right_on='RETAILER_TYPE_retailer_type_code', how='left')
    output_RETAILER_SITE.drop('RETAILER_TYPE_retailer_type_code', axis=1, inplace=True)
    output_RETAILER_SITE = pd.merge(output_RETAILER_SITE, retailer_site, left_on='RETAILER_retailer_code',
                                    right_on='RETAILER_SITE_retailer_code', how='left')
    output_RETAILER_SITE.drop('RETAILER_retailer_code', axis=1, inplace=True)

    logger.info(f"{output_RETAILER_SITE}")

    # COURSE
    output_COURSE = course
    send_to_ssms(output_COURSE, 'COURSE', Settings.export_cursor, Settings.export_conn)

    # SATISFACTION_TYPE
    output_SATISFACTION_TYPE = satisfaction_type
    send_to_ssms(output_SATISFACTION_TYPE, 'SATISFACTION_TYPE', Settings.export_cursor, Settings.export_conn)

    # SATISFACTION
    output_SATISFACTION = satisfaction
    send_to_ssms(output_SATISFACTION, 'SATISFACTION', Settings.export_cursor, Settings.export_conn)


    # TRAINING
    output_training = training
    send_to_ssms(output_training, 'TRAINING', Settings.export_cursor, Settings.export_conn)

    # ORDER_METHOD
    output_order_method = order_method
    send_to_ssms(output_order_method, 'ORDER_METHOD', Settings.export_cursor, Settings.export_conn)

    Settings.export_cursor.commit()
    Settings.export_conn.close()

