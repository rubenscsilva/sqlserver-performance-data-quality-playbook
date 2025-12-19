from pathlib import Path
import pandas as pd
import sqlalchemy as sa


# =========================
SERVER = r"HIPERION"  # ou HIPERION\INSTANCIA
DATABASE = "PortfolioSQL"
CSV_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "olist"
# =========================


def make_engine():
    # Windows Auth
    conn_str = (
        "mssql+pyodbc://@"
        f"{SERVER}/{DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
        "&TrustServerCertificate=yes"
    )
    # fast_executemany acelera bem com pyodbc
    return sa.create_engine(conn_str, fast_executemany=True)


def _get_table_columns(engine, schema: str, table: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT c.name
                FROM sys.columns c
                JOIN sys.tables t ON t.object_id = c.object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = :schema AND t.name = :table
                ORDER BY c.column_id
                """
            ),
            {"schema": schema, "table": table},
        ).fetchall()
    return [r[0] for r in rows]


def load_csv(
    engine,
    csv_path: Path,
    table: str,
    schema: str,
    parse_dates: list[str] | None = None,
    rename: dict[str, str] | None = None,
    chunksize: int = 5_000,
):
    if not csv_path.exists():
        raise FileNotFoundError(f"Não achei o CSV: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)

    if rename:
        df = df.rename(columns=rename)

    if parse_dates:
        for col in parse_dates:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    # converte NaN -> None (ajuda o driver/SQL Server com NULL)
    df = df.where(pd.notnull(df), None)

    # garante que só colunas existentes na tabela serão inseridas
    cols_db = _get_table_columns(engine, schema=schema, table=table)
    df = df[[c for c in df.columns if c in cols_db]]

    try:
        # IMPORTANTE:
        # - NÃO usar method="multi" no SQL Server (estoura limite de 2100 parâmetros)
        # - chunksize menor evita statements gigantes
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method=None,
        )
    except Exception as e:
        print(f"ERRO ao inserir em {schema}.{table} a partir de {csv_path.name}")
        print("Detalhe:", repr(e))
        raise

    print(f"Loaded {schema}.{table}: {len(df):,} rows (chunksize={chunksize})")


def main():
    engine = make_engine()

    # Arquivos padrão do Olist
    orders_csv = CSV_DIR / "olist_orders_dataset.csv"
    items_csv = CSV_DIR / "olist_order_items_dataset.csv"
    cust_csv = CSV_DIR / "olist_customers_dataset.csv"
    prod_csv = CSV_DIR / "olist_products_dataset.csv"

    if not orders_csv.exists():
        # debug rápido
        print("CSV_DIR =", CSV_DIR)
        print("CSVs encontrados =", [p.name for p in CSV_DIR.glob("*.csv")])
        raise FileNotFoundError(f"Não achei: {orders_csv}. Confere o caminho CSV_DIR.")

    # Limpeza rápida (evita duplicar carga)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE olist.order_items;"))
        conn.execute(sa.text("TRUNCATE TABLE olist.orders;"))
        conn.execute(sa.text("TRUNCATE TABLE olist.customers;"))
        conn.execute(sa.text("TRUNCATE TABLE olist.products;"))

    # Ordem: dimensões -> fatos
    load_csv(engine, cust_csv, "customers", "olist", chunksize=10_000)
    load_csv(engine, prod_csv, "products", "olist", chunksize=10_000)
    load_csv(
        engine,
        orders_csv,
        "orders",
        "olist",
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        chunksize=10_000,
    )
    load_csv(
        engine,
        items_csv,
        "order_items",
        "olist",
        parse_dates=["shipping_limit_date"],
        chunksize=10_000,
    )

    # validações rápidas
    with engine.connect() as conn:
        for q in [
            "SELECT COUNT(*) AS qtd FROM olist.customers",
            "SELECT COUNT(*) AS qtd FROM olist.products",
            "SELECT COUNT(*) AS qtd FROM olist.orders",
            "SELECT COUNT(*) AS qtd FROM olist.order_items",
        ]:
            print(q, "=>", conn.execute(sa.text(q)).scalar())


if __name__ == "__main__":
    main()
