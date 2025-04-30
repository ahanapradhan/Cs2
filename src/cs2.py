import configparser
import copy
import csv
import logging
import os
import pathlib
import time
from abc import abstractmethod, ABC
from decimal import Decimal
from pathlib import Path
from typing import List

import psycopg2
import psycopg2.extras

UNMASQUE = "_unmasque_"
WORKING_SCHEMA = "unmasque"
SCALE_DOWN = "_dscale"
OK = "OK"

INT_TYPES = ['int', 'integer', 'number']
NUMERIC_TYPES = ['numeric', 'float', 'decimal', 'Decimal', 'real']
NUMBER_TYPES = INT_TYPES + NUMERIC_TYPES
TEXT_TYPES = ['char', 'character', 'character varying', 'str', 'text', 'varchar']
NON_TEXT_TYPES = ['date'] + NUMBER_TYPES

REL_ERROR = "does not exist"
ORPHAN_COLUMN = "'?column?'"
RELATION = "Relation"

DATABASE_SECTION = "database"
SUPPORT_SECTION = "support"
LOGGING_SECTION = "logging"
FEATURE_SECTION = "feature"
OPTIONS_SECTION = "options"
TABLE_SIZE_SECTION = "table_sizes"
TABLE = "table"
DATABASE = "database"
HOST = "host"
PORT = "port"
USER = "user"
PASSWORD = "password"
DBNAME = "dbname"
SCHEMA = "schema"
SCALE_FACTOR = "scale_factor"
SCALE_RETRY = "scale_retry"


def log_result(Res, logger):
    logger.debug("Result size: ", len(Res))
    if len(Res) < 10:
        for row in Res:
            logger.debug(row)


def get_result_as_tuple_1(res, result):
    for row in res:
        # Check if the whole row is None (SPJA Case)
        if all(val is None for val in row):
            continue

        # Convert all values in the row to strings and create a tuple
        row_as_tuple = tuple(str(val) for val in row)
        result.append(row_as_tuple)
    return result


def is_error(msg):
    if not isinstance(msg, str):
        return False
    l_msg = msg.lower()
    return RELATION.lower() in l_msg and REL_ERROR.lower() in l_msg


def add_header(description, result):
    if description is not None and not is_error(description):
        colnames = [desc[0] for desc in description]
        for i in range(len(colnames)):
            if colnames[i] == '?column?':  # Changing the name of column to legitimate name.
                colnames[i] = ORPHAN_COLUMN
        result.append(tuple(colnames))


def is_result_nonempty_nullfree(res, logger=None):
    if logger is not None:
        log_result(res, logger)
        # if len(res) < 15:
        #    logger.debug(res[1:])
    if res[1:] is None:
        return False
    if res[1:] == [None]:
        return False
    if not len(res[1:]):
        return False
    for row in res[1:]:
        if all(val not in [None, 'None'] for val in row):
            return True
    return False


def get_base_t(key_list, sizes):
    max_cs = 0
    base_t = 0
    for i in range(0, len(key_list)):
        if max_cs < sizes[key_list[i][0]]:
            max_cs = sizes[key_list[i][0]]
            base_t = i
    return base_t


def get_format_args(msg, args):
    f_msg = format(str(msg))
    f_args = [f_msg]
    for arg in args:
        f_args.append(format(str(arg)))
    f_msg = ""
    for i in range(len(f_args)):
        f_msg += "%s "
    return f_msg, f_args


class PostgresQueries:
    schema = None

    def set_schema(self, schema):
        self.schema = schema

    def drop_table(self, tab):
        return f"drop table if exists {tab};"

    def create_table_like(self, tab, ctab):
        return f"Create table if not exists {tab} (like {ctab} including indexes); "  # \

    def get_row_count(self, tab):
        return f"select count(*) from {tab};"

    def truncate_table(self, table):
        return f"Truncate Table {table};"

    def insert_into_tab_select_star_fromtab(self, tab, fromtab):
        return f"Insert into {tab} Select * from {fromtab};"


class Log(logging.Logger):
    # creating a formatter
    formatter = logging.Formatter('%(asctime)s- %(name)s - %(levelname)-8s: %(message)s')

    def __init__(self, name, level):
        super().__init__(name, level)
        self.base_path = Path(__file__).parent.parent
        log_file = (self.base_path / "unmasque.log").resolve()
        fh = logging.FileHandler(log_file, 'a')
        fh.setLevel(level)
        fh.setFormatter(self.formatter)
        self.addHandler(fh)

    def __del__(self):
        fh = self.handlers[0]
        self.removeHandler(fh)
        fh.close()

    def debug(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().debug(f_msg, *f_args)

    def error(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().error(f_msg, *f_args)

    def info(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().info(f_msg, *f_args)

    def warning(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().info(f_msg, *f_args)


class AbstractConnectionHelper:
    def __init__(self, config=None, **kwargs):
        self.config = config
        if self.config is not None:
            self.config.parse_config()
        """
        If configs come from the caller (e.g. UI), prioritize it
        """
        for key, value in kwargs.items():
            if key == DBNAME:
                self.config.dbname = value
            elif key == HOST:
                self.config.host = value
            elif key == PORT:
                self.config.port = value
            elif key == USER:
                self.config.user = value
            elif key == PASSWORD:
                self.config.password = value
            elif key == SCHEMA:
                self.config.user_schema = value

        self.conn = None
        self.queries = None

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    def closeConnection(self):
        if self.conn is not None:
            if not self.get_cursor().closed:
                self.get_cursor().close()
            self.conn.close()
            self.conn = None

    @abstractmethod
    def connectUsingParams(self):
        pass

    def getConnection(self):
        if self.conn is None:
            self.connectUsingParams()
        return self.conn

    def execute_sql(self, sqls, logger=None):
        cur = self.get_cursor()
        self.cus_execute_sqls(cur, sqls, logger)

    def execute_sqls_with_DictCursor(self, sqls, logger=None):
        cur = self.get_DictCursor()
        self.cus_execute_sqls(cur, sqls, logger)

    def execute_sql_fetchone_0(self, sql, logger=None):
        cur = self.get_cursor()
        return self.cur_execute_sql_fetch_one_0(cur, sql, logger)

    def execute_sql_with_DictCursor_fetchone_0(self, sql, logger=None):
        cur = self.get_DictCursor()
        return self.cur_execute_sql_fetch_one_0(cur, sql, logger)

    @abstractmethod
    def execute_sql_fetchall(self, sql, logger=None):
        pass

    def get_cursor(self):
        cur = self.conn.cursor()
        return cur

    def get_DictCursor(self):
        pass

    @abstractmethod
    def cus_execute_sqls(self, cur, sqls, logger=None):
        pass

    @abstractmethod
    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
        pass


class Config:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # default values
        self.use_index = False
        self.scale_retry = 0
        self.sf = 1  # Default 1
        self.database = "postgres"
        # self.index_maker = "create_indexes.sql"
        self.pkfk = "pkfkrelations.csv"
        self.schema = WORKING_SCHEMA
        self.user_schema = "public"
        self.dbname = "tpcds"
        self.port = "5432"
        self.password = "postgres"
        self.user = "postgres"
        self.host = "localhost"
        self.log_level = 'DEBUG'
        self.base_path = Path(__file__).parent.parent
        self.config_loaded = False
        self.scale_down = True
        self.database = "postgres"
        self.table_sizes_dict = {}

    def parse_config(self):
        if self.config_loaded:
            return

        try:
            config_file = (self.base_path / "config.ini").resolve()
            config_object = configparser.ConfigParser()
            with open(config_file, "r") as file_object:
                config_object.read_file(file_object)
                self.database = config_object.get(DATABASE_SECTION, DATABASE)
                self.host = config_object.get(DATABASE_SECTION, HOST)
                self.port = config_object.get(DATABASE_SECTION, PORT)
                self.user = config_object.get(DATABASE_SECTION, USER)
                self.password = config_object.get(DATABASE_SECTION, PASSWORD)
                self.dbname = config_object.get(DATABASE_SECTION, DBNAME)
                self.user_schema = config_object.get(DATABASE_SECTION, SCHEMA)
                i = 1
                while True:
                    try:
                        table_size_entry = config_object.get(TABLE_SIZE_SECTION, TABLE + str(i))
                        key, value = table_size_entry.split(":")
                        self.table_sizes_dict[key.strip()] = int(value.strip())
                        i = i + 1
                    except Exception:
                        break

                self.pkfk = config_object.get(SUPPORT_SECTION, "pkfk")

                self.log_level = "DEBUG"

                try:
                    scale_factor = config_object.get(OPTIONS_SECTION, SCALE_FACTOR)
                    self.sf = int(scale_factor)
                except:
                    pass

                try:
                    retry_scale_down = config_object.get(OPTIONS_SECTION, SCALE_RETRY)
                    self.scale_retry = int(retry_scale_down)
                except:
                    pass

        except FileNotFoundError:
            print("config.ini not found. Default configs loaded!")
        except KeyError:
            print(" config not found. Using default config!")

        self.config_loaded = True


class PostgresConnectionHelper(AbstractConnectionHelper):

    def rollback_transaction(self):
        self.execute_sql(["ROLLBACK;"])

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.paramString = f"dbname={self.config.dbname} user={self.config.user} password={self.config.password} " \
                           f"host={self.config.host} port={self.config.port}"
        self.queries = PostgresQueries()
        self.config.config_loaded = True

    def begin_transaction(self):
        self.execute_sql(["BEGIN;"])

    def commit_transaction(self):
        self.execute_sql(["COMMIT;"])

    def connectUsingParams(self, single_worker=False):
        self.conn = psycopg2.connect(self.paramString)
        with self.conn.cursor() as set_cur:
            if single_worker:
                set_cur.execute("SET max_parallel_workers_per_gather = 0;")

    def cus_execute_sql_with_params(self, cur, sql, params, logger=None):
        for param in params:
            if logger is not None:
                logger.debug(sql, param)
            try:
                cur.execute(sql, param)
            except Exception as e:
                if logger is not None:
                    logger.error(str(e))

    def execute_sql_fetchall(self, sql, logger=None):
        cur = self.get_cursor()
        if logger is not None:
            logger.debug("..cur execute.." + sql)
        try:
            cur.execute(sql)
            res = cur.fetchall()
            des = cur.description
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
            des = str(e)
            raise ValueError(des)
        return res, des

    def get_DictCursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def cus_execute_sqls(self, cur, sqls, logger=None):
        # print(cur)
        for sql in sqls:
            if logger is not None:
                logger.debug("..cur execute.." + sql)
            try:
                cur.execute(sql)
            except psycopg2.ProgrammingError as e:
                # print(e)
                if logger is not None:
                    logger.error(e)
                    logger.error(e.diag.message_detail)
            # print("..done")
            except ValueError as e:
                raise e
            except Exception as e:
                if logger is not None:
                    logger.error(e)
                raise e

    def cur_execute_sql_fetch_one_0(self, cur, sql, logger=None):
        prev = None
        if logger is not None:
            logger.debug(sql)
        try:
            cur.execute(sql)
            prev = cur.fetchone()
            prev = prev[0]
            if isinstance(prev, Decimal):
                prev = float(prev)
        except psycopg2.ProgrammingError as e:
            if logger is not None:
                logger.error(e)
                logger.error(e.diag.message_detail)
        return prev


class Base:
    _instance = None
    method_call_count = 0

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Base, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper: AbstractConnectionHelper, name: str, all_sizes=None):
        if all_sizes is None:
            all_sizes = {}
        self.all_sizes = all_sizes
        self.all_relations = []
        self.connectionHelper = connectionHelper
        self.extractor_name = name
        self.local_start_time = None
        self.local_end_time = None
        self.local_elapsed_time = None
        self.done = False
        self.result = None
        self.logger = Log(name, connectionHelper.config.log_level)
        self.error = None

    def doJob(self, *args):
        self.local_start_time = time.time()
        try:
            self.result = self.doAppCountJob(args)
        except Exception as e:
            self.logger.error(str(e))
            self.done = False
            return str(e)
        else:
            self.done = True
            return self.result
        finally:
            self.local_end_time = time.time()
            self.local_elapsed_time = self.local_end_time - self.local_start_time
            self.method_call_count += 1

    @abstractmethod
    def doActualJob(self, args=None):
        pass

    def doAppCountJob(self, args):
        return self.doActualJob(args)

    @abstractmethod
    def extract_params_from_args(self, args):
        pass

    def remove_footprint(self):
        self.connectionHelper.execute_sql([f"Drop Schema if exists {self.connectionHelper.config.schema} cascade;"],
                                          self.logger)

    def _create_working_schema(self):
        self.remove_footprint()
        self.connectionHelper.execute_sql([f"Create Schema {self.connectionHelper.config.schema};"], self.logger)

    def get_fully_qualified_table_name(self, table):
        return f"{self.connectionHelper.config.schema}.{table}"

    def get_original_table_name(self, table):
        return f"{self.connectionHelper.config.user_schema}.{table}"

    def set_all_relations(self, relations: List[str]):
        self.all_relations.extend(copy.copy(relations))

    def get_all_sizes(self):
        for tab in self.all_relations:
            row_count = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.get_row_count(self.get_original_table_name(tab)))
            if row_count is None:
                self.logger.debug(f"{tab} does not exists!")
                row_count = 0
            self.all_sizes[tab] = row_count
        return self.all_sizes


class Initiator(Base):

    def extract_params_from_args(self, args):
        return args

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Initiator")
        # Ensure base_path is correctly set in the configuration
        base_path = connectionHelper.config.base_path
        if base_path is None:
            raise ValueError("base_path in configuration cannot be None")
        # Convert base_path to a Path object if it's not already one
        self.resource_path = pathlib.Path(base_path) if not isinstance(base_path, pathlib.Path) else base_path
        self.pkfk_file_path = (self.resource_path / connectionHelper.config.pkfk).resolve()
        self.schema = connectionHelper.config.schema
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.error = None
        self.downer = None

    def reset(self):
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.all_relations = []
        self.error = None

    def verify_support_files(self):
        check_pkfk = os.path.isfile(self.pkfk_file_path)
        if not check_pkfk:
            self.logger.error("Unmasque Error: \n Support File Not Accessible. ")
        return check_pkfk

    def __scale_down(self, args=None):
        self.downer = ScaleDown(self.connectionHelper, self.all_sizes, self.all_relations, self.global_key_lists)
        try:
            check = self.downer.doJob(args)
            self.all_sizes = copy.deepcopy(self.downer.sizes)
            if not check:
                self.logger.error("Scale down did not succeed! Try with larger retry factor..")
                return False
            return True
        except Exception as e:
            self.logger.error("Some error while Cs2 to scale down!!")
            return False

    def doActualJob(self, args=None):
        self.reset()
        check = self.verify_support_files()
        self.logger.info("support files verified..")
        if not check:
            return False
        all_pkfk = self.get_all_pkfk()
        self.__make_pkfk_complete_graph(all_pkfk)
        self.__do_refinement()
        self.logger.info("loaded pk-fk..", all_pkfk)
        self._create_working_schema()
        self.logger.info(f"Working schema set to {self.connectionHelper.config.schema}")
        self.__get_rows_in_tables()
        check = self.__scale_down(args)
        return check

    def __get_rows_in_tables(self):
        if not self.connectionHelper.config.table_sizes_dict.keys():
            self.get_all_sizes()
        else:
            self.all_sizes = self.connectionHelper.config.table_sizes_dict.copy()
            # for key, value in self.connectionHelper.config.table_sizes_dict:
            #     self.all_sizes[key] = value
        self.logger.debug(self.all_sizes)

    def __do_refinement(self):
        self.global_key_lists = [list(filter(lambda val: val[0] in self.all_relations, elt)) for elt in
                                 self.global_key_lists if elt and len(elt) > 1]

    def __make_pkfk_complete_graph(self, all_pkfk):
        all_relations = []
        temp = []
        for row in all_pkfk:
            if len(row) < 6:
                continue
            all_relations.append(row[0])
            if row[2].upper() == 'Y':
                self.global_pk_dict[row[0]] = self.global_pk_dict.get(row[0], '') + ("," if row[0] in temp else '') + \
                                              row[1]
                temp.append(row[0])
            found_flag = False
            for elt in self.global_key_lists:
                if (row[0], row[1]) in elt or (row[4], row[5]) in elt:
                    if (row[0], row[1]) not in elt and row[1]:
                        elt.append((row[0], row[1]))
                    if (row[4], row[5]) not in elt and row[4]:
                        elt.append((row[4], row[5]))
                    found_flag = True
                    break
            if found_flag:
                continue
            if row[0] and row[4]:
                self.global_key_lists.append([(row[0], row[1]), (row[4], row[5])])
            elif row[0]:
                self.global_key_lists.append([(row[0], row[1])])
        self.set_all_relations(list(set(all_relations)))
        self.all_relations.sort()
        self.logger.debug("all relations: ", self.all_relations)

    def get_all_pkfk(self):
        with open(self.pkfk_file_path, 'rt') as f:
            data = csv.reader(f)
            all_pkfk = list(data)[1:]
        return all_pkfk


class NullFreeExecutable(Base):
    def __init__(self, connectionHelper, name="Executable"):
        super().__init__(connectionHelper, name)
        self.data_schema = None

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        if self.data_schema is None:
            f_query = f"set search_path='{self.connectionHelper.config.schema}'; {query}"
        else:
            f_query = f"set search_path='{self.data_schema}'; {query}"
        result = []
        try:
            start_t = time.time()
            res, description = self.connectionHelper.execute_sql_fetchall(f_query, self.logger)
            end_t = time.time()
            self.logger.debug(f"Exe time: {str(end_t - start_t)}")
            add_header(description, result)
            if res is not None:
                result = get_result_as_tuple_1(res, result)
        except Exception as error:
            self.logger.error("Executable could not be run. Error: " + str(error))
            raise error
        return result

    def isQ_result_nonEmpty_nullfree(self, Res):
        return is_result_nonempty_nullfree(Res, self.logger)


class AppExtractorBase(Base, ABC):

    def __init__(self, connectionHelper, name, all_sizes=None):
        super().__init__(connectionHelper, name, all_sizes)
        self.app = NullFreeExecutable(self.connectionHelper)
        self.app_calls = 0
        self.enabled = True
        self.dirty_name = UNMASQUE + self.extractor_name

    def set_data_schema(self, schema_name=None):
        if schema_name is None:
            self.app.data_schema = self.connectionHelper.config.schema
        else:
            self.app.data_schema = schema_name

    def doAppCountJob(self, args):
        if self.enabled:
            self.app_calls = self.app.method_call_count
            self.result = self.doActualJob(args)
            self.app_calls = self.app.method_call_count - self.app_calls
            return self.result
        self.logger.debug("Feature is not enabled!")
        return True


class Cs2(AppExtractorBase):
    sample_per_multiplier = 2
    sample = {}
    TOTAL_perc = 100
    MAX_iter = 3

    def __init__(self, connectionHelper,
                 all_sizes,
                 core_relations,
                 global_key_lists, perc_based_cutoff=False, name="cs2", sf=1):
        super().__init__(connectionHelper, name)
        self.sf = sf
        self.seed_sample_size_per = 0.16 / self.sf
        self.passed = False
        self.iteration_count = 0
        self.core_relations = core_relations
        self.global_key_lists = global_key_lists
        self.sizes = all_sizes
        self.enabled = False
        self.all_relations = list(self.sizes.keys())
        self.all_relations.sort()
        self.perc_based_cutoff = perc_based_cutoff

    def _dont_stop_trying(self):
        if self.perc_based_cutoff:
            return self.seed_sample_size_per < self.TOTAL_perc
        else:
            return self.iteration_count < self.MAX_iter

    def __getSizes_cs(self):
        for table in self.all_relations:
            self.sizes[table] = self.connectionHelper.execute_sql_with_DictCursor_fetchone_0(
                self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(table)),
                self.logger)
        return self.sizes

    def extract_params_from_args(self, args):
        self.iteration_count = 0
        return args[0]

    def _truncate_tables(self):
        for table in self.core_relations:
            self.connectionHelper.execute_sql(
                [self.connectionHelper.queries.truncate_table(self.get_fully_qualified_table_name(table))], self.logger)

    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        to_truncate = True
        while self._dont_stop_trying():
            self.connectionHelper.begin_transaction()
            done = self._correlated_sampling(query, self.sizes, to_truncate)
            self.connectionHelper.commit_transaction()
            to_truncate = False  # first time truncation is sufficient, each for each union flow
            if not done:
                self.logger.info(f"sampling failed on attempt no: {self.iteration_count}")
                self.seed_sample_size_per *= self.sample_per_multiplier
                self.iteration_count = self.iteration_count + 1
            else:
                self.passed = True
                self.logger.info("Sampling is successful!")
                self.logger.info(f"Sampling Percentage: {self.seed_sample_size_per}")
                sizes = self.__getSizes_cs()
                self.logger.info(sizes)
                return True

        self._restore()
        return False

    def _restore(self):
        for table in self.core_relations:
            self.connectionHelper.execute_sqls_with_DictCursor([
                self.connectionHelper.queries.create_table_like(
                    self.get_fully_qualified_table_name(table), self.get_original_table_name(table)),
                self.connectionHelper.queries.insert_into_tab_select_star_fromtab(
                    self.get_fully_qualified_table_name(table), self.get_original_table_name(table))], self.logger)
            self.connectionHelper.commit_transaction()

    def _sanity_check(self, sizes, query):
        # check for null free rows and not just nonempty results
        new_result = self.app.doJob(query)
        # self.logger.debug(f"result after sampling: {new_result}")
        if not self.app.isQ_result_nonEmpty_nullfree(new_result):
            for table in self.core_relations:
                self.connectionHelper.execute_sqls_with_DictCursor([self.connectionHelper.queries.drop_table(
                    self.get_fully_qualified_table_name(table))], self.logger)
                self.sample[table] = sizes[table]
            return False
        return True

    def _correlated_sampling(self, query, sizes, to_truncate=False):
        self.logger.debug("Starting correlated sampling ")
        self._do_sampling(sizes, to_truncate)
        sanity = self._sanity_check(sizes, query)
        return sanity

    def _do_sampling(self, sizes, to_truncate):
        # choose base table from each key list> sample it> sample remaining tables based on base table
        for table in self.all_relations:
            self.connectionHelper.execute_sqls_with_DictCursor(
                [self.connectionHelper.queries.create_table_like(self.get_fully_qualified_table_name(table),
                                                                 self.get_original_table_name(table))], self.logger)
        if to_truncate:
            self._truncate_tables()
        self.__do_for_key_lists(sizes)
        not_sampled_tables = copy.deepcopy(self.core_relations)
        self.__do_for_empty_key_lists(not_sampled_tables)
        for table in self.core_relations:
            res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(
                self.get_fully_qualified_table_name(table)), self.logger)
            self.logger.debug(table, res)
            self.sample[table] = res

    def __do_for_empty_key_lists(self, not_sampled_tables):
        if not len(self.global_key_lists):
            for table in not_sampled_tables:
                self.connectionHelper.execute_sqls_with_DictCursor(
                    [
                        f"insert into {self.get_fully_qualified_table_name(table)} select * from "f"{self.get_original_table_name(table)} "
                        f"tablesample system({self.seed_sample_size_per});"])
                res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(
                    self.get_fully_qualified_table_name(table)))
                self.logger.debug(table, res)

    def __do_for_key_lists(self, sizes):
        for key_list in self.global_key_lists:
            base_t = get_base_t(key_list, sizes)

            # Sample base table
            base_table, base_key = key_list[base_t][0], key_list[base_t][1]
            if base_table in self.core_relations:
                limit_row = sizes[base_table]
                self.connectionHelper.execute_sqls_with_DictCursor([
                    f"insert into {self.get_fully_qualified_table_name(base_table)} "
                    f"(select * from {self.get_original_table_name(base_table)} "
                    f"tablesample system({self.seed_sample_size_per}) where ({base_key}) "
                    f"not in (select distinct({base_key}) from {self.get_fully_qualified_table_name(base_table)}) Limit {limit_row} );"],
                    self.logger)
                res = self.connectionHelper.execute_sql_fetchone_0(
                    self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(base_table)))
                self.logger.debug(base_table, res)

            # sample remaining tables from key_list using the sampled base table
            for key_item in key_list:
                sampled_table, key = key_item[0], key_item[1]

                # if sampled_table in not_sampled_tables:
                if sampled_table != base_table and sampled_table in self.core_relations:
                    limit_row = sizes[sampled_table]
                    self.connectionHelper.execute_sqls_with_DictCursor([
                        f"insert into {self.get_fully_qualified_table_name(sampled_table)} (select * from "
                        f"{self.get_original_table_name(sampled_table)} "
                        f"where {key} in (select distinct({base_key}) from {self.get_fully_qualified_table_name(base_table)}) and {key} "
                        f"not in (select distinct({key}) from {self.get_fully_qualified_table_name(sampled_table)}) Limit {limit_row}) ;"],
                        self.logger)
                    res = self.connectionHelper.execute_sql_fetchone_0(
                        self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(sampled_table)))
                    self.logger.debug(sampled_table, res)


class ScaleDown(Cs2):
    def __init__(self, connectionHelper,
                 all_sizes,
                 core_relations,
                 global_key_lists):
        super().__init__(connectionHelper, all_sizes, core_relations, global_key_lists, True, "Scale Down",
                         connectionHelper.config.sf)
        self.downscale_schema = f"{WORKING_SCHEMA}{SCALE_DOWN}"
        self.full_schema = self.connectionHelper.config.user_schema
        self.enabled = self.connectionHelper.config.scale_down
        self.seed_sample_size_per = self.seed_sample_size_per * pow(self.sample_per_multiplier,
                                                                    connectionHelper.config.scale_retry)
        print(f"seed_sample_size_per {self.seed_sample_size_per}")

    def extract_params_from_args(self, args):
        return args[0]

    def __create_schema(self):
        self.connectionHelper.execute_sql([f"Create Schema {self.downscale_schema};"], self.logger)

    def __delete_schema(self):
        self.connectionHelper.execute_sql([f"Drop Schema if exists {self.downscale_schema} cascade;"],
                                          self.logger)

    def get_fully_qualified_table_name(self, table):
        return f"{self.downscale_schema}.{table}"

    def _restore(self):
        # self.__delete_schema()
        pass

    def doAppCountJob(self, args):  # no need to app count for scaling down
        if not self.enabled:
            return True
        self.__delete_schema()
        self.__create_schema()
        self.set_data_schema(self.downscale_schema)
        for table in self.core_relations:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.create_table_like(
                self.get_fully_qualified_table_name(table), self.get_original_table_name(table))], self.logger)
        check = self.doActualJob(self.extract_params_from_args(args))
        if not check:
            self.set_data_schema()
        else:
            self.logger.info("Hopefully Scaling Down Worked!")
            print("Hopefully Scaling Down Worked!")
            self.connectionHelper.config.user_schema = self.downscale_schema
            print(self.seed_sample_size_per)
            print(self.sample)
        return check

    def _correlated_sampling(self, query, sizes, to_truncate=False):
        self.logger.debug("Starting scaling down  sampling ")
        self._do_sampling(sizes, to_truncate)
        sanity = self._sanity_check(sizes, query)
        self.logger.debug(f"Sampling status: {sanity}")
        return sanity


if __name__ == '__main__':
    config = Config()
    config.parse_config()
    query_path = os.path.join(config.base_path, 'query.sql')
    with open(query_path, 'r') as file:
        content = file.read()

    query = content
    print(query)
    conn = PostgresConnectionHelper(config)
    conn.connectUsingParams()
    init = Initiator(conn)
    init.doJob(query)
    conn.closeConnection()
