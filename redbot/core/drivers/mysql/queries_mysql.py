#https://github.com/Cog-Creators/Red-DiscordBot/pull/4198
import json
from typing import Tuple, List

from ..base import BaseDriver, IdentifierData, ConfigCategory
class mysql_queries(object):
    def __init__(self,schema_name):
        self.schema_name=schema_name
        
    def encode_identifier_data(
            self,
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
    def create_redcogs(self):
        return """
CREATE TABLE IF NOT EXISTS `red_cogs` (
  `cog_name` varchar(255) COLLATE utf8mb4_ja_0900_as_cs_ks NOT NULL,
  `cog_id` varchar(255) COLLATE utf8mb4_ja_0900_as_cs_ks NOT NULL,
  `cog_category` varchar(255) COLLATE utf8mb4_ja_0900_as_cs_ks NOT NULL,
  `tablename` varchar(255) COLLATE utf8mb4_ja_0900_as_cs_ks NOT NULL,
  PRIMARY KEY (`cog_name`(255),`cog_id`(255),`cog_category`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs_ks;
"""
    def create_cogtable(self,id_data: encode_identifier_data):
        keytype = "varchar(255) COLLATE utf8mb4_ja_0900_as_cs_ks " if id_data[-1] == 1 else "bigint"
        primary_index = "(255)" if id_data[-1] == 1 else ""
    
        out_sql="CREATE TABLE IF NOT EXISTS {table_name} (\n"+\
         ''.join(["`primary_key_{num}` {keytype} NOT NULL,\n".format(num=num,keytype=keytype) for num in range(id_data[-2])])+ \
        "`json_data` json DEFAULT NULL,\n"+\
        "PRIMARY KEY (" + ",".join(["`primary_key_{num}`{primary_index}".format(num=num, primary_index=primary_index) for num in range(id_data[-2])]) + ")\n"+\
         ")ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs_ks;\n"
        out_sql=out_sql.format(table_name="_".join([id_data[0],id_data[1],id_data[2]] if len(id_data[2])!=0 else [id_data[0],id_data[1]]),keytype=keytype)
        out_sql+="INSERT IGNORE INTO red_cogs (cog_name,cog_id,cog_category,tablename) VALUES ({val_str});\n".format(val_str=",".join(["\""+st+"\""for st in[id_data[0],id_data[1],id_data[2],"_".join([id_data[0],id_data[1],id_data[2]] if len(id_data[2])!=0 else [id_data[0],id_data[1]])]]))
        return out_sql
    def detect_table(self,id_data: encode_identifier_data):
        select_key=id_data[-4]
        keylen=id_data[-2]# if len( select_key)!=0 and  select_key[0]!="0" else 0
        where_arg = ["`primary_key_{num}` = {select_key}".format(num=num,  select_key= select_key[num]) for num in range(keylen)] if keylen != 0 else"TRUE"
        where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
        return "select exists (select * FROM {table_name} WHERE {where_arg});".format(
            table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
            where_arg=where_arg)
        

    def get_query(self,id_data: encode_identifier_data):
        identifiers_base=id_data[-3]
        identifier_string = "$"
        if len(identifiers_base):
            identifier_string += "." + ".".join(identifiers_base)
        pkeys=id_data[-4]
        # keylen=id_data[-2] if len( select_key)!=0 and  select_key[0]!="0" else 0
        # where_arg = ["`primary_key_{num}` = {select_key}".format(num=num,  select_key= select_key[num]) for num in range(keylen)] if keylen != 0 else"TRUE"
        # where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
        primary_key_len =id_data[-2]
        num_missing_pkeys = primary_key_len-len(pkeys)
        where_arg = ["`primary_key_{num}` = {select_key}".format(num=num,  select_key= select_key) for num,select_key in enumerate(pkeys)] if len(pkeys) != 0  else"TRUE"
        where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
        if num_missing_pkeys==0:
            # if len(identifiers_base):
            #     return "SELECT json_extract(json_data,{path}) FROM {table_name} WHERE {where_arg};".format(
            #         table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
            #         where_arg=where_arg, path=identifier_string)
            # else:
            #     return "SELECT json_data FROM {table_name} WHERE {where_arg};".format(
            #         table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
            #         where_arg=where_arg)
            return "SELECT json_extract(json_data,'{path}') FROM {table_name} WHERE {where_arg};".format(
                table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
                where_arg=where_arg, path=identifier_string)
        elif num_missing_pkeys==1:
            return "SELECT JSON_OBJECTAGG(primary_key_{key_num}, json_data) FROM {table_name} WHERE {where_arg};".format(key_num=primary_key_len-1, table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
                where_arg=where_arg)
        else:
            concat_pkstr=[]
            for i in range(len(pkeys),primary_key_len):
                if len(concat_pkstr)==0:
                    concat_pkstr.append("\"\'{\"\'")
                else:
                    concat_pkstr.append("\'\":{\"\'")
                concat_pkstr.append("primary_key_{num}".format(num=i))
            concat_pkstr.append("\'\":\'")
            concat_pkstr.append("json_data")
            concat_pkstr.append("\'"+"".join(["}" for i in range(num_missing_pkeys)])+"\'")
            output_str="set @num=1,@index_num=1;\n"+\
                "with RECURSIVE\n"+\
                "	temptb(id,concat_json) AS (\n"+\
                "		select @num := @num + 1,concat("+",".join(concat_pkstr)+") from {table_name} WHERE {where_arg}\n"+\
                "),jsontb(now_id,max_id,json_col) AS\n"+\
                "(\n"+\
                "SELECT 1,(select max(id) from temptb),json_object()\n"+\
                "UNION ALL\n"+\
                "select @index_num := @index_num + 1 as now_id,max_id,JSON_MERGE_PATCH(json_col, (select concat_json from temptb WHERE id=@index_num)) from jsontb where now_id=@index_num and @index_num<max_id\n"+\
                ")\n"+\
                "SELECT json_col\n"+\
                "FROM jsontb where now_id=@index_num;\n"
            return output_str.format(table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
                where_arg=where_arg)
                
        
    def get_type_query(self,id_data: encode_identifier_data):
        identifiers_base=id_data[-3]
        identifier_string = "$"
        if len(identifiers_base):
            identifier_string += "." + ".".join(identifiers_base)
        select_key=id_data[-4]
        keylen=id_data[-2]# if len( select_key)!=0 and  select_key[0]!="0" else 0
        where_arg = ["`primary_key_{num}` = {select_key}".format(num=num,  select_key= select_key[num]) for num in range(keylen)] if keylen != 0 else"TRUE"
        where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
        if len(identifiers_base):
            return "SELECT json_type(json_extract(json_data,{path})) FROM {table_name} WHERE {where_arg};".format(
                table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
                where_arg=where_arg,path=identifier_string)
        else:
            return "SELECT json_type(json_data) FROM {table_name} WHERE {where_arg};".format(table_name="_".join([id_data[0],id_data[1],id_data[2]] if len(id_data[2])!=0 else [id_data[0],id_data[1]]), where_arg=where_arg)
        
    # def set_query(self,id_data: encode_identifier_data):
    #     #      -- Delete all documents which we're setting first, since we don't know whether they'll be
    #     #      -- replaced by the subsequent INSERT.
    #     #怖い
    #     identifiers_base=id_data[-3]
    #     identifier_string = "$"
    #     if len(identifiers_base):
    #         identifier_string += "." + ".".join(identifiers_base)
    #     select_key=id_data[-4]
    #     keylen=id_data[-2]# if len( select_key)!=0 and  select_key[0]!="0" else 0
    #     where_arg = ["`primary_key_{num}` = {select_key}".format(num=num,  select_key= select_key[num]) for num in range(keylen)]# if keylen != 0 else"TRUE"
    #     where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
    #     key_name=["`primary_key_{num}`".format(num=num) for num in range(keylen)]+["json_data"]
    #     val_str=
    #     if len(identifiers_base):
    #         return "SELECT json_type(json_extract(json_data,{path})) FROM {table_name} WHERE {where_arg};".format(
    #             table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
    #             where_arg=where_arg,path=identifier_string)
    #     else:
    #         return "SELECT json_type(json_data) FROM {table_name} WHERE {where_arg};".format(table_name="_".join([id_data[0],id_data[1],id_data[2]] if len(id_data[2])!=0 else [id_data[0],id_data[1]]), where_arg=where_arg)

    def set_query(self, id_data: encode_identifier_data, val):
        # print(id_data)
        # print(val)
        keytype = "varchar(255)" if id_data[-1] == 1 else "bigint"
        identifiers_base = id_data[-3]
        identifier_string = "$"
        if len(identifiers_base):
            identifier_string += "." + ".".join(identifiers_base)
        pkeys = id_data[-4]

        primary_key_len = id_data[-2]
        num_missing_pkeys = primary_key_len - len(pkeys)
        where_arg = ["`primary_key_{num}` = \"{select_key}\"".format(num=num, select_key=select_key) for num, select_key in
                     enumerate(pkeys)] if len(pkeys) != 0 else "TRUE"
        set_arg_p=where_arg
        where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
        key_name=["primary_key_{num}".format(num=num) for num in range(primary_key_len)]+["json_data"]

        if num_missing_pkeys == 0:
            concat_jsonstr = []
            if not isinstance(val,(str,int)) and len(val)!=1:
                if isinstance(val,dict):
                    for i in range(len(identifiers_base)):
                        if len(concat_jsonstr) == 0:
                            concat_jsonstr.append("\'{\"")
                        else:
                            concat_jsonstr.append("\":{\"")
                        concat_jsonstr.append(identifiers_base[i])
                    concat_jsonstr.append("\":\"" if len(identifiers_base) else "\'")
                    # print(val)
                    # print(len(val))
                    cj = json.dumps(val, ensure_ascii=False)#str(val)# if isinstance(val, (str, int)) else "[" + ",".join(["\"" + str(st) + "\"" for st in val]) + "]"
                    concat_jsonstr.append(cj)
                    if len(identifiers_base):
                        concat_jsonstr.append("\'" + "".join(["}" for i in range(len(identifiers_base))]) + "\'")
                    else:
                        concat_jsonstr.append("".join(["}" for i in range(len(identifiers_base))]) + "\'")
                        # print("1111")
                    # print(concat_jsonstr)
                    insert_jsonstr = "".join(concat_jsonstr) if len(concat_jsonstr) != 0 else "\'{\"" + str(concat_jsonstr[0]) + "\"}\'"
                else:
                    for i in range(len(identifiers_base)):
                        if len(concat_jsonstr) == 0:
                            concat_jsonstr.append("\'{\"")
                        else:
                            concat_jsonstr.append("\":{\"")
                        concat_jsonstr.append(identifiers_base[i])
                    concat_jsonstr.append("\":\"" if isinstance(val, (str, int)) else "\":")
                    # print(val)
                    # print(len(val))
                    cj = json.dumps(val, ensure_ascii=False) if isinstance(val, (str, int)) else "[" + ",".join(["\"" + str(st) + "\"" for st in val]) + "]"
                    concat_jsonstr.append(cj)
                    if isinstance(val, (str, int)):
                        concat_jsonstr.append("\"" + "".join(["}" for i in range(len(identifiers_base))]) + "\'")
                    else:
                        concat_jsonstr.append("".join(["}" for i in range(len(identifiers_base))]) + "\'")
                        # print("1111")
                    # print(concat_jsonstr)
                    insert_jsonstr = "".join(concat_jsonstr) if len(concat_jsonstr) != 0 else "\'{\"" + str(concat_jsonstr[0]) + "\"}\'"
            else:
                # insert_jsonstr="\'{}\'"
                for i in range(len(identifiers_base)):
                    if len(concat_jsonstr) == 0:
                        concat_jsonstr.append("\'{\"")
                    else:
                        concat_jsonstr.append("\":{\"")
                    concat_jsonstr.append(identifiers_base[i])
                concat_jsonstr.append("\":" if isinstance(val, (str, int)) and len(identifiers_base)!=0 else '\'')
                # print(val)
                # print(len(val))
                cj = val#json.dumps(val, ensure_ascii=False) if isinstance(val, (str, int)) else "[" + ",".join(["\"" + str(st) + "\"" for st in val]) + "]"
                concat_jsonstr.append(cj)
                if isinstance(val, (str, int)):
                    concat_jsonstr.append("" + "".join(["}" for i in range(len(identifiers_base))]) + "\'")
                else:
                    concat_jsonstr.append("".join(["}" for i in range(len(identifiers_base))]) + "\'")
                    # print("1111")
                # print(concat_jsonstr)
                insert_jsonstr = "".join(concat_jsonstr) if len(concat_jsonstr) != 0 else "\'{\"" + str(concat_jsonstr[0]) + "\"}\'"
            merge_jsonstr = ",".join(set_arg_p)
            merge_jsonstr += ", json_data = JSON_MERGE_PATCH(json_data,kawaii.kawaii_json_data)"
            return self.create_cogtable(id_data)+"INSERT INTO {table_name} ({key_name}) VALUES({pkeys},{insert_jsonstr}) as kawaii({kawaii_name}) ON DUPLICATE KEY UPDATE {merge_jsonstr};".format(
                table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
                key_name=",".join(key_name),pkeys=",".join(["\""+pks+"\"" for pks in pkeys]),insert_jsonstr=insert_jsonstr,merge_jsonstr=merge_jsonstr,kawaii_name=",".join(["kawaii_primary_key_{num}".format(num=num) for num in range(primary_key_len)]+["kawaii_json_data"])
            )
        else:
            if len(val)!=0 and val != "{}":
                json_pkeylist = ["json_pkey_" + str(i) for i in range(len(pkeys), primary_key_len)]
                # print("json_key")
                # print(json_pkeylist)
                json_pkeylist_rev = list(reversed(json_pkeylist))
                select_str = "SELECT "
                select_str += ",".join(pkeys) + "," if len(pkeys) else ""
                select_str += ",".join(json_pkeylist)
                select_str += "," + str(json_pkeylist[-1] + "_value") + "\n"
                select_str += "from(\n"
                for i in range(len(json_pkeylist)):
                    if i == (len(json_pkeylist) - 1):
                        select_str += "SELECT {skey},JSON_EXTRACT('{json_in}', CONCAT('$.', {skey})) AS {skey}_value from \n".format(
                            skey=json_pkeylist[0], json_in=val)
                    else:
                        skeys = ",".join(json_pkeylist[0:-(i + 1)])
                        select_str += "select {skeys},JSON_EXTRACT({ext_one}_value, CONCAT('$.', {ext_two})) AS {ext_two}_value from(\n".format(
                            skeys=skeys, ext_one=json_pkeylist_rev[i + 1], ext_two=json_pkeylist_rev[i])
                for i in range(len(json_pkeylist)):
                    if i == (len(json_pkeylist) - 1):
                        if i == 0:
                            select_str += "JSON_TABLE( JSON_KEYS('{json_in}'),\'$[*]\' COLUMNS ({arg_one} {keytype} PATH \'$\')) AS {arg_one}_list) AS {arg_one}_table".format(json_in=val,arg_one=json_pkeylist[i],keytype=keytype)
                        else:
                            select_str += "JSON_TABLE( JSON_KEYS({arg_one}_value),\'$[*]\' COLUMNS ({arg_two} {keytype} PATH \'$\')) AS {arg_two}_list)AS {arg_two}_table".format(
                                arg_one=json_pkeylist[i - 1], arg_two=json_pkeylist[i], keytype=keytype)
                    elif i == 0:
                        select_str += "JSON_TABLE( JSON_KEYS('{json_in}'),\'$[*]\' COLUMNS ({arg_one} {keytype} PATH \'$\')) AS {arg_one}_list) AS {arg_one}_table,\n".format(
                            json_in=val, arg_one=json_pkeylist[i], keytype=keytype)
                    else:
                        select_str += "JSON_TABLE( JSON_KEYS({arg_one}_value),\'$[*]\' COLUMNS ({arg_two} {keytype} PATH \'$\')) AS {arg_two}_list)AS {arg_two}_table,\n".format(
                            arg_one=json_pkeylist[i - 1], arg_two=json_pkeylist[i], keytype=keytype)
                out_str = "DELETE FROM {table_name} WHERE {where_arg};\n".format(where_arg=where_arg, table_name="_".join(
                    [id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]))
                out_str += "INSERT INTO {table_name} ({key_name}) {select_str} ON DUPLICATE KEY UPDATE json_data=VALUES(json_data);".format(
                    key_name=",".join(key_name), select_str=select_str,
                    table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]])
                )

                return self.create_cogtable(id_data)+out_str#.format(table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]))
            else:
                return self.create_cogtable(id_data)
        
    def clear_query(self, id_data: encode_identifier_data):
        keytype = "varchar(255)" if id_data[-1] == 1 else "bigint"
        identifiers_base = id_data[-3]
        identifier_string = "$"
        if len(identifiers_base):
            identifier_string += "." + ".".join(identifiers_base)
        pkeys = id_data[-4]

        primary_key_len = id_data[-2]
        num_missing_pkeys = primary_key_len - len(pkeys)
        where_arg = ["`primary_key_{num}` = {select_key}".format(num=num, select_key=select_key) for num, select_key in
                     enumerate(pkeys)] if len(pkeys) != 0 else "TRUE"
        set_arg_p=where_arg
        where_arg = where_arg if not isinstance(where_arg, list) else " AND ".join(where_arg)
        key_name=["`primary_key_{num}`".format(num=num) for num in range(primary_key_len)]+["json_data"]
        if len(identifiers_base):
            return "UPDATE {table_name} SET json_data=json_remove(json_data,{path}) WHERE {where_arg};".format(
                table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),
                where_arg=where_arg, path=identifier_string)
        elif len(pkeys):
            return "DELETE FROM {table_name} WHERE {where_arg};\n".format(table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]),where_arg=where_arg)
        elif(id_data[2] is not None and id_data[2] != ''):
            out_sql="DROP TABLE {table_name}\n".format(table_name="_".join([id_data[0], id_data[1], id_data[2]] if len(id_data[2]) != 0 else [id_data[0], id_data[1]]))
            out_sql += "DELETE FROM red_cogs WHERE cog_name = {cog_name} AND cog_id = {cog_id} AND cog_category = {cog_category};".format(cog_name=id_data[0], cog_id=id_data[1], cog_category=id_data[2])
            return out_sql
        else:
            """
            https://stackoverflow.com/questions/11053116/mysql-bulk-drop-table-where-table-like
            """
            out_sql="SET @tables = NULL;\n"
            out_sql+="SELECT GROUP_CONCAT(table_schema, \'.`\', table_name, \'`\') INTO @tables FROM\n"
            out_sql+="(select * from\n"
            out_sql+="information_schema.tables\n"
            out_sql+="WHERE table_schema = \'{schema_name}\' AND table_name LIKE \'{tbname}\_%\'\n".format(schema_name=self.schema_name,tbname="\_".join([id_data[0], id_data[1]]))
            out_sql+="LIMIT 10) TT;\n"
            out_sql+="SET @tables = CONCAT(\'DROP TABLE \', @tables);\n"
            out_sql+="select @tables;\n"
            out_sql+="PREPARE stmt1 FROM @tables;\n"
            out_sql+="EXECUTE stmt1;\n"
            out_sql+="DEALLOCATE PREPARE stmt1;\n"
            out_sql += "DELETE FROM red_cogs WHERE cog_name = {cog_name} AND cog_id = {cog_id};".format(cog_name=id_data[0], cog_id=id_data[1])
            return out_sql
        
    def all_clear_query(self):

            """
            https://stackoverflow.com/questions/11053116/mysql-bulk-drop-table-where-table-like
            """
            out_sql="SET @tables = NULL;\n"
            out_sql+="SELECT GROUP_CONCAT(table_schema, \'.`\', table_name, \'`\') INTO @tables FROM\n"
            out_sql+="(select {schema_name} as table_schema,tablename as table_name from\n".format(schema_name=self.schema_name)
            out_sql+="red_cogs\n"
            out_sql+=") TT;\n"
            out_sql+="SET @tables = CONCAT(\'DROP TABLE \', @tables);\n"
            out_sql+="select @tables;\n"
            out_sql+="PREPARE stmt1 FROM @tables;\n"
            out_sql+="EXECUTE stmt1;\n"
            out_sql+="DEALLOCATE PREPARE stmt1;\n"
            out_sql += "DROP TABLE `red_cogs`;"
            return out_sql
            


"INSERT INTO {table_name} ({key_name}) VALUES ({val_str}) ON DUPLICATE KEY UPDATE json_data = json_set(json_data, :path, VALUES(json_data));"

_create_table = """
CREATE TABLE IF NOT EXISTS {table_name} (
    data JSON
);
"""

_get_query = """
SELECT json_extract(target_table.data, :path)
FROM {table_name} as target_table
"""
_get_type_query = """
SELECT json_type(target_table.data, :path)
FROM {table_name} as target_table
"""

_set_query = """
UPDATE {table_name}
  set data = json_set(data, :path, json(:value))
"""

_clear_query = """
UPDATE {table_name}
  set data = json_remove(data, :path)
"""

_prep_query = """
INSERT INTO {table_name} (data)
SELECT '{{}}'
WHERE NOT EXISTS (SELECT * FROM {table_name})
"""