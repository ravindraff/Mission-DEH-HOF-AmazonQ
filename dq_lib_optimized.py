from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, upper, count, row_number, concat_ws, concat,
    to_date, to_json, add_months, current_date, map_concat,
    array_distinct, collect_list, broadcast, expr, reduce
)
from pyspark.sql.types import StringType, BooleanType, StructType
from pyspark.sql.window import Window
from typing import List, Literal


class DataQualityRulesChecker(DataQualityChecker):

    def __init__(self, df: DataFrame, nm: str, tableLayer: str = 'Silver'):
        self.fileName    = ' '
        self.folderPath  = ' '
        self.check_flag  = 'check_status'
        self.name        = nm
        self.tableLayer  = tableLayer

        df = self.__add_unique_id(df)
        self.__save_snapshot(df)
        self.__apply_master_data_filtering()

    # ── Private Helpers ────────────────────────────────────────────────────────

    def __add_unique_id(self, df: DataFrame) -> DataFrame:
        print("Adding a unique ID column...")
        return df.withColumn("_temp_unique_id", generate_uuid())

    def __save_snapshot(self, df: DataFrame):
        print("Saving a snapshot of the dataframe...")
        if self.tableLayer == "Silver":
            stgTblName = f'{dq_source_schema}.silver_staging_{dq_sub_env}_{self.name}'
            df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(stgTblName)
            self.data      = spark.table(stgTblName)
            self.row_count = self.data.count()
            print(f"Snapshot saved to table {stgTblName}")
        elif self.tableLayer == "Bronze":
            self.data      = df.persist()
            self.row_count = self.data.count()
            print("Snapshot saved to memory")

    def __del__(self):
        try:
            print("Cleaning up DataQualityRulesChecker class")
            self.data.unpersist()
        except Exception:
            print("Cleaning up DataQualityRulesChecker class - no data to clean up")

    def __apply_master_data_filtering(self):
        print("Applying master data filtering...")
        if self.row_count == 0 or "environment" not in self.data.columns:
            return

        rows_before   = self.row_count
        master_tables = self.__get_list_of_master_tables()
        if master_tables is None:
            print(f"No master tables found for the current object {self.name}.")
            return

        filtered_df = self.data
        for _, row in master_tables.iterrows():
            filtered_col  = row['filtered_column']
            curated_col   = row['curated_column']
            master_table  = row['curated_object_against']
            master_df     = self.__get_master_table_items(master_table, curated_col)
            if master_df is None:
                continue

            before      = filtered_df.count()
            filtered_df = filtered_df.join(
                master_df,
                filtered_df[filtered_col] == master_df[curated_col],
                'inner'
            ).select(filtered_df["*"])
            print(f"Filtered {before - filtered_df.count()} records based on [{filtered_col}].")

        self.__save_snapshot(filtered_df)
        print(f"Filtered dataset: {self.row_count} records ({rows_before - self.row_count} removed).")

    def __get_list_of_master_tables(self, filter_in_silver: int = 0):
        field  = '[filter_in_silver]' if filter_in_silver else '[filter_in_pre_curated]'
        query  = (f"SELECT * FROM [dq].[o9_data_quality_master_data_filtering] "
                  f"WHERE [object_name] = '{self.name}' AND {field} = 1")
        df     = self.__load_data(query, isSql=True)
        if df is None or df.count() == 0:
            print(f"No master tables found for {self.name}.")
            return None
        return df.toPandas()

    def __get_master_table_items(self, master_table: str, curated_column: str):
        queries = self.__get_queries(master_table, curated_column)
        if not queries:
            print(f"Master table extraction not implemented for '{master_table}'.")
            return None
        df = self.__load_data(queries['main'])
        if df is not None:
            return df
        print(f"Master table {master_table} empty. Trying fallback...")
        df = self.__load_data(queries['fallback'])
        if df is None:
            print(f"Failed to load main and fallback for '{master_table}'.")
        return df

    def __get_queries(self, master_table: str, curated_column: str) -> dict:
        # Base template — main and fallback use same query for all tables
        _base = {
            "item_master": (
                f"SELECT `{curated_column}` FROM cotydatacloud_{{env}}.{{schema}}.o9_gold_item_master "
                f"WHERE OPERATION_TYPE <> 'D' AND O9_scope = 'In O9' GROUP BY {curated_column}"
            ),
            "item_master_extended": (
                f"SELECT `{curated_column}` FROM cotydatacloud_{{env}}.{{schema}}.o9_gold_item_master_extended "
                f"WHERE OPERATION_TYPE <> 'D' AND O9_scope IN ('In O9', 'In DRP') GROUP BY {curated_column}"
            ),
            "account_master_agg_sold_to": (
                f"SELECT `{curated_column}` FROM cotydatacloud_{{env}}.{{schema}}.o9_gold_account_master_agg_sold_to "
                f"WHERE OPERATION_TYPE <> 'D' GROUP BY {curated_column}"
            ),
            "account_master_agg_sold_to_forecast_reporting": (
                f"SELECT `{curated_column}` FROM cotydatacloud_{{env}}.{{schema}}.o9_gold_account_master_agg_sold_to_forecast_reporting "
                f"WHERE OPERATION_TYPE <> 'D' GROUP BY {curated_column}"
            ),
        }
        if master_table not in _base:
            return None
        query = _base[master_table].format(env=environment, schema=source_schema)
        return {'main': query, 'fallback': query}

    def __load_data(self, query: str, isSql: bool = False):
        try:
            if isSql:
                df = (spark.read.format("sqlserver")
                      .option("host",     hostdb)
                      .option("port",     portdb)
                      .option("user",     userdb)
                      .option("password", passworddb)
                      .option("database", databasedb)
                      .option("query",    query)
                      .load())
                df.cache()
            else:
                df = spark.sql(query)
            return df if df.count() > 0 else None
        except Exception as e:
            print(f"Error loading data: {e}")
            return None

    def __build_check_cols(self, check_value_expr, flag: str, col_name: str, col_value_expr, error_type: str) -> DataFrame:
        """Add standard DQ check columns to a DataFrame."""
        return (self.data
                .withColumn("check_value",        check_value_expr)
                .withColumn("check_passed",        lit(flag))
                .withColumn("check_column_value",  col_value_expr)
                .withColumn("check_column_name",   lit(col_name))
                .withColumn("object_name",         lit(self.name))
                .withColumn("error_type",          lit(error_type)))

    def __split_ok_fail(self, temp_df: DataFrame, drop_cols: List[str] = None):
        """Split a tagged DataFrame into (ok_df, fail_df) and strip helper columns from ok_df."""
        _std = ["check_value", "check_passed", "check_column_name", "check_column_value", "object_name", "error_type"]
        drop_cols = _std + (drop_cols or [])
        if not temp_df.head(1):
            return temp_df, temp_df
        df_ok   = temp_df.filter(col("check_value") == "OK")
        df_fail = temp_df.filter(col("check_value") == "FAIL")
        return self.clean_ok_output(df_ok.drop(*drop_cols)), self.report_extract(df_fail)

    # ── Public Check Methods ───────────────────────────────────────────────────

    def check_nulls(self, is_warning: bool, cols_null: List, dummy: str = "") -> tuple:
        col_null   = cols_null[0]
        flag       = "check_nulls_warning" if is_warning else "check_nulls"
        error_type = "WARNING" if is_warning else "CRITICAL"

        check_expr = when(col(col_null).isNull() | (col(col_null) == ""), lit("FAIL")).otherwise(lit("OK"))
        temp_df    = self.__build_check_cols(check_expr, flag, col_null, col(col_null), error_type)

        if not temp_df.head(1):
            return self.clean_ok_output(temp_df), self.report_extract(temp_df)

        df_fail = temp_df.filter(col("check_value") == "FAIL")
        if is_warning:
            temp_df = temp_df.replace("FAIL", "OK")
        df_ok = temp_df.filter(col("check_value") == "OK")

        if is_warning:
            df_ok = self.resolve_warning(df_ok, df_fail, col_null, dummy)

        _drop = ["check_value", "check_passed", "check_column_name", "check_column_value", "object_name", "error_type"]
        return self.clean_ok_output(df_ok.drop(*_drop)), self.report_extract(df_fail)

    def check_duplicates_with_PKs(self, is_warning: bool, cols_uniqueness_values: List,
                                   PK_values: List = [], delete_by_path: bool = False) -> tuple:
        data             = self.data
        original_columns = data.columns
        flag             = "check_duplicates"
        error_type       = "WARNING" if is_warning else "CRITICAL"

        # Resolve PKs
        if PK_values:
            PKs_index = list(PK_values)
        else:
            DQ_PK     = DQ_table.toPandas()
            list_PK   = DQ_PK[DQ_PK["object_name"] == self.name]
            PKs_index = list_PK["PK"].values.tolist()

        col_uniqueness = cols_uniqueness_values[0]
        PKs_index.append(col_uniqueness)

        # Uppercase PK columns for case-insensitive dedup
        string_cols = {f.name for f in data.schema.fields if isinstance(f.dataType, StringType)}
        for c in PKs_index:
            data = data.withColumn(
                c + "_upper",
                upper(col(c)) if c in string_cols else col(c)
            )
        PKs_upper = [c + "_upper" for c in PKs_index]

        # Find duplicated values
        dup_values = (data.select(PKs_upper)
                      .groupBy(PKs_upper)
                      .agg(count("*").alias("dup_count"))
                      .filter(col("dup_count") > 1)
                      .select(col_uniqueness + '_upper')
                      .toPandas()[col_uniqueness + '_upper']
                      .tolist())

        # Null-safe PK check
        data = data.withColumn("check_null", lit("OK"))
        for pk in PKs_index[:-1]:  # exclude uniqueness col
            data = (data
                    .withColumn(pk, when(col(pk).isNull(), lit("")).otherwise(col(pk)))
                    .withColumn("check_null", when(col(pk).isNull(), lit("FAIL")).otherwise(col("check_null"))))

        colname = "Multiple PKs" if len(PKs_index) > 1 and col_uniqueness in PKs_index else col_uniqueness

        # Window for deduplication — oldest for split_temp, newest otherwise
        order_dir = F.asc if "split_temp" in self.name else F.desc
        w = Window.partitionBy(PKs_upper).orderBy(
            order_dir("_timestamp"),
            *[col(c) for c in original_columns if c not in PKs_upper and c != "_timestamp"]
        )

        _std_cols = [
            lit(flag).alias("check_passed"),
            col(col_uniqueness).alias("check_column_value"),
            lit(colname).alias("check_column_name"),
            lit(self.name).alias("object_name"),
            lit(error_type).alias("error_type")
        ]

        temp_df_ok = (data
                      .select('*', data.check_null.alias("check_value"))
                      .drop("check_null")
                      .select('*', row_number().over(w).alias("rn"), *_std_cols)
                      .filter("rn = 1")
                      .drop("rn"))

        fail_select = ['*', row_number().over(w).alias("rn"),
                       lit("FAIL").alias("check_value"), *_std_cols]

        if delete_by_path:
            fail_select.insert(2, concat_ws(
                " -- Duplicates Folder : ",
                array_distinct(collect_list("source_file_name").over(w))
            ).alias("source_file_name_agg"))

        temp_df_fail = (data
                        .select(*fail_select)
                        .filter("rn > 1")
                        .drop("rn", "check_null")
                        .dropDuplicates(PKs_upper))

        if delete_by_path:
            temp_df_fail = temp_df_fail.withColumn(
                "source_file_name_agg",
                concat(lit("Raw Folder : "), col("source_file_name_agg"))
            )

        cols_to_drop = [c for c in data.columns if c.endswith("_upper")]
        _drop_ok     = ["check_value", "check_passed", "check_column_name", "check_column_value", "object_name", "error_type"]

        if temp_df_ok.head(1):
            df_ok   = temp_df_ok.filter(col("check_value") == "OK").drop(*cols_to_drop)
            df_fail = (temp_df_fail.filter(col("check_value") == "FAIL").drop(*cols_to_drop)
                       if temp_df_fail.head(1) else temp_df_fail.drop(*cols_to_drop))
        else:
            df_ok   = temp_df_ok.drop(*cols_to_drop)
            df_fail = (temp_df_fail.filter(col("check_value") == "FAIL").drop(*cols_to_drop)
                       if temp_df_fail.head(1) else temp_df_fail.drop(*cols_to_drop))

        return self.clean_ok_output(df_ok.drop(*_drop_ok)), self.report_extract(df_fail)

    def check_isin_value(self, is_warning: bool, cols_isin_values: List,
                         keep_in_successful_output: bool = False) -> tuple:
        data       = self.data
        flag       = "check_isin_value"
        error_type = "WARNING" if is_warning else "CRITICAL"

        c           = cols_isin_values[0]
        column_name = c[0]
        value_list  = c[1]

        remove_check_column = False
        list_is_dataframe   = isinstance(value_list, (DataFrame, ConnectDataFrame))

        # Prepare lookup DataFrame if value_list is a DataFrame
        if list_is_dataframe:
            temp_col_name  = column_name if isinstance(column_name, list) else [column_name]
            value_list_df  = (value_list
                              .select(column_name)
                              .distinct()
                              .withColumn("concatenated_values", concat_ws(" / ", *temp_col_name))
                              .select("concatenated_values")
                              .cache())

        # Concatenate multi-column names
        if isinstance(column_name, list):
            data        = data.withColumn(column_name := " / ".join(column_name),
                                          concat_ws(" / ", *[col(c) for c in column_name]))
            remove_check_column = True

        # Standard isin / join check
        if len(c) < 3:
            if list_is_dataframe:
                temp_df = (data.alias("d")
                           .join(broadcast(value_list_df).alias("v"),
                                 col(f"d.{column_name}") == col("v.concatenated_values"),
                                 "left_outer")
                           .select("*",
                                   when(col("concatenated_values").isNotNull() & col(column_name).isNotNull(), lit("OK"))
                                   .otherwise(lit("FAIL")).alias("check_value"),
                                   lit(flag).alias("check_passed"),
                                   col(column_name).alias("check_column_value"),
                                   lit(column_name).alias("check_column_name"),
                                   lit(self.name).alias("object_name"),
                                   lit(error_type).alias("error_type")))
            else:
                temp_df = (data.select("*",
                                       when(col(column_name).isin(value_list) & col(column_name).isNotNull(), lit("OK"))
                                       .otherwise(lit("FAIL")).alias("check_value"),
                                       lit(flag).alias("check_passed"),
                                       col(column_name).alias("check_column_value"),
                                       lit(column_name).alias("check_column_name"),
                                       lit(self.name).alias("object_name"),
                                       lit(error_type).alias("error_type")))

            temp_df.cache()

            df_ok   = temp_df if keep_in_successful_output else temp_df.filter(col("check_value") == "OK")
            df_fail = temp_df.filter(col("check_value") == "FAIL").drop("concatenated_values")

            _drop   = ["check_value", "check_passed", "check_column_name", "concatenated_values"]
            cleaned = df_ok.drop(*_drop)
            if remove_check_column:
                cleaned = cleaned.drop(column_name)

            return self.clean_ok_output(cleaned), self.report_extract(df_fail)

        # Schema-based isin check
        if len(c) == 3 and isinstance(c[2], StructType):
            distinct_df = spark.createDataFrame(data=c[1], schema=c[2])
            join_cond   = data[c[0]] == distinct_df[distinct_df.schema.fieldNames()[0]]
            _std = dict(check_passed=lit(flag), check_column_value=lit(c[0]),
                        check_column_name=lit(c[0]), object_name=lit(self.name), error_type=lit(error_type))

            df_ok = (data.join(distinct_df, join_cond, "left_semi")
                     .withColumn("check_value", lit("OK"))
                     .withColumns(_std))
            df_fail = (data.join(distinct_df, join_cond, "left_anti")
                       .withColumn("check_value", lit("FAIL"))
                       .withColumns(_std))

            return self.clean_ok_output(df_ok), self.report_extract(df_fail)

    def check_is_number(self, cols_number: List) -> tuple:
        col_name   = cols_number[0]
        nullable   = cols_number[1] == "YES"
        allow_neg  = len(cols_number) > 2 and cols_number[2] == "YES"
        flag       = "check_is_number"
        error_type = "CRITICAL"

        regex = r'^-?(\d+(\.\d*)?|\.\d+)$' if allow_neg else r'^(\d+(\.\d*)?|\.\d+)$'

        check_expr = when(col(col_name).rlike(regex) | col(col_name).isNull() | (col(col_name) == ""), lit("OK")).otherwise(lit("FAIL"))
        if not nullable:
            check_expr = when(col(col_name).isNull() | (col(col_name) == ""), lit("FAIL")).otherwise(check_expr)

        temp_df = (self.data
                   .withColumn("check_value",       check_expr)
                   .withColumn("check_passed",       lit(flag))
                   .withColumn("check_column_value", col(col_name))
                   .withColumn("check_column_name",  lit(col_name))
                   .withColumn("object_name",        lit(self.name))
                   .withColumn("idpipeline",         col("idpipeline"))
                   .withColumn("error_type",         lit(error_type)))

        return self.__split_ok_fail(temp_df)

    def check_is_date(self, cols_date: List) -> tuple:
        col_name   = cols_date[0]
        nullable   = cols_date[1] == "YES"
        flag       = "check_is_date"
        error_type = "CRITICAL"

        try:
            dateformat  = cols_date[2]
            date_format = dateformat
        except Exception:
            dateformat  = r'^[1-9]\d{3}\.\d{2}\.\d{2}$'
            date_format = 'yyyy.MM.dd'

        check_expr = when(
            (col(col_name).rlike(dateformat) & to_date(col(col_name), date_format).isNotNull()) |
            col(col_name).isNull() | (col(col_name) == ""),
            lit("OK")
        ).otherwise(lit("FAIL"))

        if not nullable:
            check_expr = when(col(col_name).isNull() | (col(col_name) == ""), lit("FAIL")).otherwise(check_expr)

        temp_df = (self.data
                   .withColumn("check_value",       check_expr)
                   .withColumn("check_passed",       lit(flag))
                   .withColumn("check_column_value", col(col_name))
                   .withColumn("check_column_name",  lit(col_name))
                   .withColumn("object_name",        lit(self.name))
                   .withColumn("idpipeline",         col("idpipeline"))
                   .withColumn("error_type",         lit(error_type)))

        return self.__split_ok_fail(temp_df)

    def check_funny_characters(self, cols_funny_characters: List) -> tuple:
        col_name      = cols_funny_characters[0]
        flag          = "check_funny_characters"
        error_type    = "CRITICAL"
        special_chars = ['^', ';', ',', '|']
        regex         = '\\'.join(special_chars)

        temp_df = (self.data
                   .withColumn("check_value",       when(col(col_name).rlike(rf"[\{regex}]"), lit("FAIL")).otherwise(lit("OK")))
                   .withColumn("check_passed",       lit(flag))
                   .withColumn("check_column_value", col(col_name))
                   .withColumn("check_column_name",  lit(col_name))
                   .withColumn("object_name",        lit(self.name))
                   .withColumn("idpipeline",         col("idpipeline"))
                   .withColumn("error_type",         lit(error_type)))

        return self.__split_ok_fail(temp_df)

    def check_correct_pattern(self, column_to_check: str, regex_pattern: str,
                               mode: Literal["match_correct", "match_incorrect"] = "match_correct",
                               remove_marker: bool = False) -> tuple:
        flag       = "check_correct_pattern"
        error_type = "CRITICAL"

        match_ok = "OK" if mode == "match_correct" else "FAIL"
        match_fail = "FAIL" if mode == "match_correct" else "OK"

        temp_df = (self.data
                   .withColumn("check_value",
                               when(F.regexp_instr(col(column_to_check), F.lit(rf"{regex_pattern}")) > 0, lit(match_ok))
                               .otherwise(lit(match_fail)))
                   .withColumn("check_passed",       lit(flag))
                   .withColumn("check_column_value", col(column_to_check))
                   .withColumn("check_column_name",  lit(column_to_check))
                   .withColumn("object_name",        lit(self.name))
                   .withColumn("idpipeline",         col("idpipeline"))
                   .withColumn("error_type",         lit(error_type)))

        if remove_marker:
            temp_df = (temp_df
                       .withColumn(column_to_check,    F.regexp_replace(col(column_to_check), F.lit(rf"{regex_pattern}"), "$1"))
                       .withColumn("check_column_value", F.regexp_replace(col(column_to_check), F.lit(rf"{regex_pattern}"), "$1")))

        return self.__split_ok_fail(temp_df)

    def check_master_filters(self) -> tuple:
        master_tables = self.__get_list_of_master_tables(filter_in_silver=1)
        flag          = "check_master_filters"
        error_type    = "CRITICAL"

        if master_tables is None:
            print(f"No master filtering table found for {self.name}")
            return self.data, self.data.limit(0)

        filtered_df      = (self.data
                            .withColumn("object_name", F.lit(self.name))
                            .withColumn("error_type",  F.lit(error_type)))
        all_failed_records = []

        for _, row in master_tables.iterrows():
            filtered_col    = row['filtered_column']
            curated_col     = row['curated_column']
            master_table    = row['curated_object_against']
            where_condition = row['object_where_condition'] or '1=1'

            master_df = spark.sql(f"SELECT * FROM {master_table}")
            before    = filtered_df.count()

            success_df = (filtered_df
                          .join(master_df.alias(master_table.split('.')[-1]),
                                filtered_df[filtered_col] == master_df[curated_col], 'inner')
                          .filter(where_condition)
                          .select(filtered_df["*"]))

            fail_df = (filtered_df.exceptAll(success_df)
                       .withColumn('check_passed',       F.lit(flag))
                       .withColumn('check_column_name',  F.lit(filtered_col))
                       .withColumn('check_column_value', F.col(filtered_col)))

            if fail_df.head(1):
                all_failed_records.append(fail_df)

            filtered_df = success_df
            print(f"Filtered out {before - filtered_df.count()} records based on [{filtered_col}] where {where_condition}.")

        df_fail = (reduce(DataFrame.unionByName, all_failed_records)
                   if all_failed_records else fail_df)

        return self.clean_ok_output(filtered_df), self.report_extract(df_fail)

    def apply_bronze_isin_checks(self, environment: str, catalog_schema: str) -> tuple:
        isin_checks_df = DQ_isin_checks_table.filter(F.col('bronze_sink_table_name') == self.name)

        if isin_checks_df.count() == 0:
            print(f"No bronze Is-In checks found for {self.name}")
            return [], []

        success_dfs, fail_dfs = [], []

        def _process_row(row, is_sql: bool):
            bronze_col    = row['bronze_column']
            lookup_col    = row['lookup_column']
            lookup_table  = row['lookup_object_against']
            allowed_vals  = row['allowed_values']
            where_cond    = row['object_where_condition'] or '1=1'
            allowed_str   = '\n'.join([f"UNION SELECT '{v}' " for v in allowed_vals.split(', ')]) if allowed_vals else ''

            if is_sql:
                lookup_df = self.__load_data(
                    f"SELECT DISTINCT {lookup_col} AS {bronze_col} FROM {lookup_table} WHERE {where_cond} {allowed_str}"
                )
            else:
                if lookup_table:
                    lookup_df = spark.sql(
                        f"SELECT DISTINCT {lookup_col} AS {bronze_col} "
                        f"FROM cotydatacloud_{environment}.{catalog_schema}.{lookup_table} "
                        f"WHERE {where_cond} {allowed_str}"
                    )
                else:
                    union_str = '\nUNION '.join([f"SELECT '{v}' AS {bronze_col}" for v in allowed_vals.split(', ')]) if allowed_vals else ''
                    lookup_df = spark.sql(union_str)

            s, f = self.check_isin_value(False, [[bronze_col, lookup_df]])
            success_dfs.append(s)
            fail_dfs.append(f)

        for row in isin_checks_df.filter(F.col('lookup_object_is_in_dbr_catalog') == 1).collect():
            _process_row(row, is_sql=False)
        for row in isin_checks_df.filter(F.col('lookup_object_is_in_sql_server') == 1).collect():
            _process_row(row, is_sql=True)

        return success_dfs, fail_dfs

    def check_row_to_delete(self) -> DataFrame:
        data    = self.data
        columns = data.columns

        temp_df = data.withColumn(
            "check_value",
            when(reduce(lambda x, y: x | y, [col(c) == "row_to_delete" for c in columns]), lit("DELETE"))
            .otherwise(lit("KEEP"))
        )
        deletes = temp_df.filter(col("check_value") == "DELETE")
        return deletes.drop('_temp_unique_id', 'check_value') if deletes.head(1) else None

    def build_final_output(self, *args) -> DataFrame:
        _drop   = ["check_value", "check_passed", "check_column_name", "_temp_unique_id"]
        df_join = args[0].drop(*_drop)
        for df in args[1:]:
            df_join = df_join.intersect(df.drop(*_drop))
        return df_join

    def build_final_fail(self, *args) -> DataFrame:
        df_join = args[0]
        for df in args[1:]:
            df_join = df_join.unionByName(df)

        try:
            df_join = df_join.dropDuplicates()
        except AttributeError as e:
            print(f"Duplicates could not be dropped: {e}")

        # Enrich json_row_data with file name and path
        df_join = (df_join
                   .withColumn("json_row_data", expr("from_json(json_row_data, 'map<string,string>')"))
                   .withColumn("json_row_data", map_concat(
                       col("json_row_data"),
                       expr(f"map('file', '{self.fileName}', 'path', '{self.folderPath}')")))
                   .withColumn("json_row_data", to_json(col("json_row_data"))))
        return df_join

    def check_date_generic(self, rule_type: str, column_name: str = None,
                           start_col: str = None, end_col: str = None,
                           months: int = 36, date_format: str = "yyyy.MM.dd") -> tuple:
        """
        Generic date validation.
        rule_type='single' → column_name <= today + months
        rule_type='pair'   → start_col <= end_col
        """
        data       = self.data
        error_type = "CRITICAL"

        if rule_type == "single":
            if not column_name:
                raise ValueError("column_name is required for single-date rule")
            if column_name not in data.columns:
                print(f"{column_name} column not found")
                return data, data.limit(0)

            flag   = "check_date_range"
            cutoff = add_months(current_date(), months)

            temp_df = (data
                       .withColumn("parsed_date", to_date(col(column_name), date_format))
                       .withColumn("check_value",
                                   when(col("parsed_date").isNotNull() & (col("parsed_date") <= cutoff), lit("OK"))
                                   .otherwise(lit("FAIL")))
                       .withColumn("check_passed",       lit(flag))
                       .withColumn("check_column_name",  lit(column_name))
                       .withColumn("check_column_value", col(column_name))
                       .withColumn("object_name",        lit(self.name))
                       .withColumn("idpipeline",         col("idpipeline"))
                       .withColumn("error_type",         lit(error_type)))

            return self.__split_ok_fail(temp_df, drop_cols=["parsed_date"])

        elif rule_type == "pair":
            if not start_col or not end_col:
                raise ValueError("start_col and end_col are required for pair-date rule")
            for c in [start_col, end_col]:
                if c not in data.columns:
                    print(f"{c} column not found")
                    return data, data.limit(0)

            flag = "check_date_pair"

            temp_df = (data
                       .withColumn("start_date_parsed", to_date(col(start_col), date_format))
                       .withColumn("end_date_parsed",   to_date(col(end_col),   date_format))
                       .withColumn("check_value",
                                   when(col("start_date_parsed").isNotNull() &
                                        col("end_date_parsed").isNotNull() &
                                        (col("start_date_parsed") <= col("end_date_parsed")), lit("OK"))
                                   .otherwise(lit("FAIL")))
                       .withColumn("check_passed",       lit(flag))
                       .withColumn("check_column_name",  lit(f"{start_col} <= {end_col}"))
                       .withColumn("check_column_value", concat_ws(" | ", col(start_col), col(end_col)))
                       .withColumn("object_name",        lit(self.name))
                       .withColumn("idpipeline",         col("idpipeline"))
                       .withColumn("error_type",         lit(error_type)))

            return self.__split_ok_fail(temp_df, drop_cols=["start_date_parsed", "end_date_parsed"])

        else:
            raise ValueError(f"Unknown rule_type '{rule_type}'. Use 'single' or 'pair'.")
