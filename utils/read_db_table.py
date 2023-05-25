import pandas as pd
from session import get_session
from sqlalchemy import text


def get_climatology_table():
    with get_session() as Session:
        with Session() as session:
            schema_name = 'climatology'
            table_name = 'union_table'
            # Fetch the table data using a select query
            query = text(f"SELECT * FROM {schema_name}.{table_name}")
            df = pd.read_sql(query, session.get_bind().connect(), )
            # columns_to_encode = ["admin0name", "admin1name", "admin2name"]
            # for column in df[columns_to_encode]:
            #     df[column] = df[column].str.decode("utf-8-sig")
    return df