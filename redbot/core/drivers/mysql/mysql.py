import getpass
import json
import sys
from pathlib import Path
from typing import Optional, Any, AsyncIterator, Tuple, Union, Callable, List

from .queries_mysql import mysql_queries

try:
    # pylint: disable=import-error
    import aiomysql
except ModuleNotFoundError:
    aiomysql = None

from ... import data_manager, errors
from ..base import BaseDriver, IdentifierData, ConfigCategory
from ..log import log

from .queries import (
    _create_table,
    _get_query,
    _set_query,
    _clear_query,
    _prep_query,
    _get_type_query,
)

__all__ = ["MySQLDriver"]

# _PKG_PATH = Path(__file__).parent
# DDL_SCRIPT_PATH = _PKG_PATH / "mysql_ddl.sql"
# DROP_DDL_SCRIPT_PATH = _PKG_PATH / "mysql_drop_ddl.sql"


def encode_identifier_data(
    id_data: IdentifierData,
) -> Tuple[str, str, str, List[str], List[str], int, bool]:
    return (
        id_data.cog_name,
        id_data.uuid,
        id_data.category,
        ["0"] if id_data.category == ConfigCategory.GLOBAL else list(id_data.primary_key),
        list(id_data.identifiers),
        1 if id_data.category == ConfigCategory.GLOBAL else id_data.primary_key_len,
        id_data.is_custom,
    )


class MySQLDriver(BaseDriver):

    _pool: Optional["aiomysql.pool.Pool"] = None
    _query=None

    @classmethod
    async def initialize(cls, **storage_details) -> None:
        if aiomysql is None:
            raise errors.MissingExtraRequirements(
                "Red must be installed with the [mysql] extra to use the MySQL driver"
            )
        cls._query=mysql_queries(storage_details["db"])
        cls._pool = await aiomysql.create_pool(**storage_details)
        await cls._execute(cls._query.create_redcogs())
        # with DDL_SCRIPT_PATH.open() as fs:
        #     await cls._pool.execute(fs.read())

    @classmethod
    async def teardown(cls) -> None:
        if cls._pool is not None:
            # await cls._pool.close()
            cls._pool.close()
            await cls._pool.wait_closed()

    @staticmethod
    def get_config_details():
        # unixmsg = (
        #     ""
        #     if sys.platform == "win32"
        #     else (
        #         " - Common directories for MySQL Unix-domain sockets (/run/MySQL, "
        #         "/var/run/postgresl, /var/pgsql_socket, /private/tmp, and /tmp),\n"
        #     )
        # )
        host = (
            input(
                f"Enter the MySQL server's address.\n"
                f"If left blank, Red will try the following, in order:\n"
                # f" - The PGHOST environment variable,\n{unixmsg}"
                f" - localhost.\n"
                f"> "
            )
            or None
        )
        host="localhost" if host is None else host

        print(
            "Enter the MySQL server port.\n"
            "If left blank, this will default to either:\n"
            # " - The PGPORT environment variable,\n"
            " - 3306."
        )
        while True:
            port = input("> ") or None
            if port is None:
                port=3306
                break

            try:
                port = int(port)
            except ValueError:
                print("Port must be a number")
            else:
                break

        user = (
            input(
                "Enter the MySQL server username.\n"
                "If left blank, this will default to either:\n"
                # " - The PGUSER environment variable,\n"
                " - The OS name of the user running Red (ident/peer authentication).\n"
                "> "
            )
            or None
        )

        # passfile = r"%APPDATA%\MySQL\pgpass.conf" if sys.platform == "win32" else "~/.pgpass"
        password = getpass.getpass(
            f"Enter the MySQL server password. The input will be hidden.\n"
            f"  NOTE: If using ident/peer authentication (no password), enter NONE.\n"
            f"When NONE is entered, this will default to:\n"
            # f" - The PGPASSWORD environment variable,\n"
            # f" - Looking up the password in the {passfile} passfile,\n"
            f" - No password.\n"
            f"> "
        )
        if password == "NONE":
            password = None

        database = (
            input(
                "Enter the MySQL database's name.\n"
                "If left blank, this will default to either:\n"
                # " - The PGDATABASE environment variable,\n"
                " - The OS name of the user running Red.\n"
                "> "
            )
            or None
        )

        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "db": database,
            "charset":"utf8mb4",
        }

    async def get(self, identifier_data: IdentifierData):
        try:
            # result = await self._execute(
            #     "SELECT red_main.get($1)",
            #     encode_identifier_data(identifier_data),
            #     method=self._pool.fetchval,
            # )
            result=await self._execute(self._query.get_query(encode_identifier_data(identifier_data)),method="fetchone")
            # result =await out.fetchone()
        
        except aiomysql.DatabaseError:#UndefinedTableError:
            raise KeyError from None
        output=None
        result=result[0]
        if result is None:
            # The result is None both when postgres yields no results, or when it yields a NULL row
            # A 'null' JSON value would be returned as encoded JSON, i.e. the string 'null'
            raise KeyError

        try:
            output=json.loads(result)
        except TypeError:
            # if isinstance(result,(int):
            output=result
        # print(output)
        return output#json.loads(result)

    async def set(self, identifier_data: IdentifierData, value=None):
        try:
            # await self._execute(
            #     "SELECT red_main.set($1, $2::jsonb)",
            #     encode_identifier_data(identifier_data),
            #     json.dumps(value),
            # )
            await self._execute(self._query.set_query(encode_identifier_data(identifier_data),json.dumps(value,ensure_ascii=False)),method="set")
            # await
        except aiomysql.OperationalError:
            raise errors.CannotSetSubfield

    async def clear(self, identifier_data: IdentifierData):
        try:
            # await self._execute(
            #     "SELECT red_main.clear($1)", encode_identifier_data(identifier_data)
            # )
            await self._execute(
                self._query.clear_query(encode_identifier_data(identifier_data))
            )
        except aiomysql.DatabaseError:
            pass

    # async def inc(
    #     self, identifier_data: IdentifierData, value: Union[int, float], default: Union[int, float]
    # ) -> Union[int, float]:
    #     try:
    #         return await self._execute(
    #             f"SELECT red_main.inc($1, $2, $3)",
    #             encode_identifier_data(identifier_data),
    #             value,
    #             default,
    #             method=self._pool.fetchval,
    #         )
    #     except aiomysql.DataError as exc:
    #         raise errors.StoredTypeError(*exc.args)
    #
    # async def toggle(self, identifier_data: IdentifierData, default: bool) -> bool:
    #     try:
    #         return await self._execute(
    #             "SELECT red_main.inc($1, $2)",
    #             encode_identifier_data(identifier_data),
    #             default,
    #             method=self._pool.fetchval,
    #         )
    #     except aiomysql.DataError as exc:
    #         raise errors.StoredTypeError(*exc.args)

    @classmethod
    async def aiter_cogs(cls) -> AsyncIterator[Tuple[str, str]]:
        query = "SELECT cog_name, cog_id FROM red_cogs"
        log.invisible(query)
        async with cls._pool.acquire() as conn, conn.transaction():
            async for row in conn.cursor(query):
                yield row["cog_name"], row["cog_id"]

    @classmethod
    async def delete_all_data(
        cls, *, interactive: bool = False, drop_db: Optional[bool] = None, **kwargs
    ) -> None:
        """Delete all data being stored by this driver.

        Parameters
        ----------
        interactive : bool
            Set to ``True`` to allow the method to ask the user for
            input from the console, regarding the other unset parameters
            for this method.
        drop_db : Optional[bool]
            Set to ``True`` to drop the entire database for the current
            bot's instance. Otherwise, schemas within the database which
            store bot data will be dropped, as well as functions,
            aggregates, event triggers, and meta-tables.

        """
        # if interactive is True and drop_db is None:
        #     print(
        #         "Please choose from one of the following options:\n"
        #         " 1. Drop the entire MySQL database for this instance, or\n"
        #         " 2. Delete all of Red's data within this database, without dropping the database "
        #         "itself."
        #     )
        #     options = ("1", "2")
        #     while True:
        #         resp = input("> ")
        #         try:
        #             drop_db = bool(options.index(resp))
        #         except ValueError:
        #             print("Please type a number corresponding to one of the options.")
        #         else:
        #             break
        # if drop_db is True:
        #     storage_details = data_manager.storage_details()
        #     await cls._pool.execute(f"DROP DATABASE $1", storage_details["database"])
        # else:
        #     with DROP_DDL_SCRIPT_PATH.open() as fs:
        #         await cls._pool.execute(fs.read())
        await cls._execute(cls._query.all_clear_query())

    @classmethod
    async def _execute(cls, query: str, *args, method: Optional[Callable] = None) -> Any:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                # if method is None:
                #     method = cur.execute#cls._pool.execute
                log.invisible("Query: %s", query)
                if args:
                    log.invisible("Args: %s", args)
                await cur.execute(query,*args)
                if method is None:
                    return
                elif method=="fetchone":
                    return await cur.fetchone()
                elif method=="fetchall":
                    return await cur.fetchall()
                elif method=="set":
                    await conn.commit()
                    return 1
                # return await method(query, *args)
    # async def _execute(
    #     self,
    #     query: str,
    #     category: str,
    #     type_query: Optional[str] = None,
    #     path: Optional[str] = None,
    #     data: Optional[str] = ...,
    # ) -> Any:
    #     log.invisible("Query: %s", query)
    #     if category:
    #         self.db.cursor().execute(_create_table.format(table_name=category))
    #         self.db.cursor().execute(_prep_query.format(table_name=category))
    #     if type_query:
    #         obj_type = self.db.cursor().execute(type_query, (path,))
    #         obj_type = obj_type.fetchone()
    #         obj_type = obj_type[0]
    #         if obj_type is None:
    #             raise KeyError
    #     else:
    #         obj_type = ...
    #     _data = {
    #         "path": path,
    #     }
    #     if data is not ...:
    #         _data.update({"value": data})
    #     with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    #         for future in concurrent.futures.as_completed(
    #             [executor.submit(self.db.cursor().execute, query, _data)]
    #         ):
    #             output = future.result()
    #             output = output.fetchone()
    #             if obj_type is ...:
    #                 return
    #             output = output[0]
    #             if obj_type in ["true", "false"] and isinstance(output, int):
    #                 output = bool(output)
    #             elif obj_type not in ["text"] and isinstance(output, str):
    #                 output = json.loads(output)
    #
    #     return output