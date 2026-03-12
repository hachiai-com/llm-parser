"""
sql_dao.py
----------
Python DAO layer over a single MySQL connection.

  - run_query      : SELECT → List[Dict[str, str]]
  - exec_ins_query : INSERT/UPDATE/DELETE with parameterised list
  - close          : closes the underlying connection
  - db_type        : returns the db type string

Only MySQL is supported. MSSQL is stubbed and raises NotImplementedError.

Dependencies:
    pip install mysql-connector-python
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError as exc:
    raise ImportError(
        "mysql-connector-python is required.  "
        "Install it with:  pip install mysql-connector-python"
    ) from exc


MYSQL_DB = "mysql"
MSSQL_DB = "mssql"

_log = logging.getLogger(__name__)


class SqlDao:
    """
    Thin DAO layer over a single MySQL connection.

      - One connection per instance (no pool).
      - run_query      → SELECT  → List[Dict[str, str]]
      - exec_ins_query → INSERT / UPDATE / DELETE
      - close()        → conn.close()
    """

    def __init__(
        self,
        db_type: str,
        host: str,
        database: str,
        user_name: str,
        password: str,
        port: int = 3306,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Args:
            db_type   : "mysql" (or "mssql" — stubbed).
            host      : DB host, e.g. "localhost" or "192.168.1.10".
            database  : Database name.
            user_name : DB username.
            password  : DB password.
            port      : DB port (default 3306).
            logger    : Optional logger. Falls back to module-level logger.
        """
        self._logger = logger or _log
        self._db_type = db_type.lower()
        self._conn: Optional[Any] = None

        if self._db_type == MYSQL_DB:
            self._logger.info(f"Connecting to MySQL  host={host}  port={port}  db={database} ...")
            try:
                self._conn = mysql.connector.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user_name,
                    password=password,
                    use_pure=True
                )
                if self._conn.is_connected():
                    self._logger.info("MySQL connection established successfully.")
            except MySQLError as exc:
                self._logger.error(f"Failed to connect to MySQL: {exc}", exc_info=True)
                raise
            except Exception as exc:
                self._logger.error(f"Failed to connect to MySQL: {exc}", exc_info=True)
                raise

        elif self._db_type == MSSQL_DB:
            raise NotImplementedError("MSSQL support is not yet implemented.")
        else:
            msg = f"Unknown DB type: '{db_type}'"
            self._logger.error(msg)
            raise ValueError(msg)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def db_type(self) -> str:
        return self._db_type

    def run_query(
        self,
        query: str,
        parameters: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        """
        Executes a SELECT (or any statement that returns rows).

        Args:
            query      : SQL string with %s placeholders.
            parameters : Ordered list of values bound to placeholders.
                         Pass None or [] for queries with no parameters.

        Returns:
            List of rows; each row is a Dict[column_name → str_value].
            Values are always coerced to str (or None), mirroring rs.getString().
        """
        result: List[Dict[str, str]] = []
        cursor = None
        try:
            self._logger.info(f"Select Query = {query}")
            cursor = self._conn.cursor(dictionary=True)

            params = tuple(parameters) if parameters else ()
            cursor.execute(query, params)

            rows = cursor.fetchall()
            for row in rows:
                result.append(
                    {col: (str(val) if val is not None else None) for col, val in row.items()}
                )
        except MySQLError as exc:
            self._logger.error(f"run_query failed: {exc}", exc_info=True)
            raise
        finally:
            if cursor:
                cursor.close()

        return result

    def exec_ins_query(
        self,
        query: str,
        parameters_list: List[List[str]],
        show_query: bool = False,
    ) -> int:
        """
        Executes an INSERT / UPDATE / DELETE with a batched parameter list.

        Flattens all sub-lists into one sequential bind sequence, matching
        a multi-row VALUES clause built into the query string itself.

        Args:
            query           : Full SQL string (may contain multiple VALUE rows).
            parameters_list : List of parameter sub-lists, one per VALUES row.
            show_query      : When True, logs the query and params before execution.

        Returns:
            Number of rows affected.
        """
        cursor = None
        try:
            flat_params: List[str] = []
            for sub_list in parameters_list:
                if sub_list:
                    flat_params.extend(sub_list)

            cursor = self._conn.cursor()

            if show_query:
                self._logger.info(f"QUERY=>{query}<-")
                self._logger.info(f"PARAMS=>{flat_params}<-")

            st = time.time()
            cursor.execute(query, flat_params)
            self._conn.commit()
            ed = time.time()

            rows_affected = cursor.rowcount
            elapsed_ms = int((ed - st) * 1000)
            self._logger.info(
                f"{datetime.now()}]: updated {rows_affected} rows. Took {elapsed_ms} ms"
            )
            return rows_affected

        except MySQLError as exc:
            self._logger.error(f"exec_ins_query failed: {exc}", exc_info=True)
            self._conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    def close(self) -> None:
        """Closes the underlying DB connection."""
        if self._conn and self._conn.is_connected():
            try:
                self._logger.info("Closing connection to MySQL database...")
                self._conn.close()
            except MySQLError as exc:
                self._logger.error(f"Error closing connection: {exc}", exc_info=True)


# ---------------------------------------------------------------------------
# Main — connect, query row count, disconnect
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    DB_HOST     = "localhost"
    DB_PORT     = 3306
    DB_NAME     = "test"
    DB_USER     = "root"
    DB_PASSWORD = "123456"
    TABLE_NAME  = "orders_raw"
    
    print("Before connection")
    dao = SqlDao(
        db_type="mysql",
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user_name=DB_USER,
        password=DB_PASSWORD,
    )
    print("After connection")

    try:
        rows = dao.run_query(f"SELECT COUNT(*) AS total FROM {TABLE_NAME}")
        if rows:
            total = rows[0].get("total", "N/A")
            print(f"\n  Table '{TABLE_NAME}' contains {total} row(s).\n")
        else:
            print(f"\n  No result returned for table '{TABLE_NAME}'.\n")
    except Exception as exc:
        print(f"\n  Error: {exc}\n")
    finally:
        dao.close()
        print("  Database connection closed.")


if __name__ == "__main__":
    main()