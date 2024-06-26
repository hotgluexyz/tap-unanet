import pyodbc

s = ";".join([
    "DRIVER={CData Virtuality Unicode(x64)}",
    "SERVER=data.unanet.biz",
    "PORT=35433",
    "DATABASE=datavirtuality",
    "SSLMODE=require",
    "UID=ctec-corp",
    "PWD=dQNl8EgyGgb0JrqX*Cc2",
])

print(f"conn string: {s}")

c = pyodbc.connect(s)

# cursor.execute("SELECT * FROM sys.views")

table_name = 'customer'
schema_name = 'ctec_corp'

def get_possible_primary_keys():
    cursor = c.cursor()

    row = cursor.statistics(table=table_name, schema=schema_name, unique=True).fetchone()

    get_attribute_pos = lambda description, attr: next(
        (
            i
            for i, el in enumerate(description)
            if el[0] == attr
        ),
        None
    )

    possible_primary_keys = list()

    while row is not None:
        description = row.cursor_description

        index_name_pos = get_attribute_pos(description, "index_name")
        column_name_pos = get_attribute_pos(description, "column_name")

        if index_name_pos is not None and column_name_pos is not None:
            if row[index_name_pos] == f"pk_{table_name}":
                possible_primary_keys = [row[column_name_pos]]
                break
            else:
                possible_primary_keys.append(row[column_name_pos])

        row = cursor.fetchone()

    print(possible_primary_keys)

def get_table_columns():
    cursor = c.cursor()

    result = cursor.execute(f"select * from {schema_name}.{table_name} limit 1")

    row = result.fetchone()
    desc = row.cursor_description

    print(desc)


get_possible_primary_keys()