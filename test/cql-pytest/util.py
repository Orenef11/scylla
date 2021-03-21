# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
##################################################################

# Various utility functions which are useful for multiple tests.
# Note that fixtures aren't here - they are in conftest.py.

import string
import random
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from functools import cached_property


def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

# A function for picking a unique name for test keyspace or table.
# This name doesn't need to be quoted in CQL - it only contains
# lowercase letters, numbers, and underscores, and starts with a letter.
unique_name_prefix = 'cql_test_'
def unique_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)
unique_name.last_ms = 0

# A utility function for creating a new temporary keyspace with given options.
# It can be used in a "with", as:
#   with new_test_keyspace(cql, '...') as keyspace:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_keyspace(cql, opts):
    keyspace = unique_name()
    cql.execute("CREATE KEYSPACE " + keyspace + " " + opts)
    try:
        yield keyspace
    finally:
        cql.execute("DROP KEYSPACE " + keyspace)

# A utility function for creating a new temporary table with a given schema.
# It can be used in a "with", as:
#   with new_test_table(cql, keyspace, '...') as table:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_table(cql, keyspace, schema, extra=""):
    table = keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table + "(" + schema + ")" + extra)
    try:
        yield table
    finally:
        cql.execute("DROP TABLE " + table)

def project(column_name_string, rows):
    """Returns a list of column values from each of the rows."""
    return [getattr(r, column_name_string) for r in rows]


class CassandraTypes(Enum):
    ASCII = "ascii"
    BIGINT = "bigint"
    BLOB = "blob"
    BOOLEAN = "boolean"
    DATE = "date"
    DOUBLE = "double"
    DECIMAL = "decimal"
    FLOAT = "float"
    INET = "inet"
    INT = "int"
    SMALLINT = "smallint"
    TEXT = "text"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMEUUID = "timeuuid"
    TINYINT = "tinyint"
    UUID = "uuid"
    VARCHAR = "varchar"


class CassandraDataGenerator:
    """
    This class was created according to the following links:
    http://itdoc.hitachi.co.jp/manuals/3020/30203V0300e/BV030040.HTM
     and https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cql_data_types_c.html.
    This link describes how the types are defined in JAVA.
    """
    INT_HIGH_RANGE = 2 ** 31 - 1
    INT_LOW_RANGE = -2 ** 31
    SMALL_INT_HIGH_RANGE = 2 ** 15 - 1
    SMALL_INT_LOW_RANGE = -2 ** 15
    TINY_INT_HIGH_RANGE = 127
    TINY_INT_LOW_RANGE = -128
    BIG_INT_HIGH_RANGE = 2 ** 63 - 1
    BIG_INT_LOW_RANGE = -2 ** 63
    DECIMAL_HIGH_RANGE = 10 ** 38 - 1
    DECIMAL_LOW_RANGE = -10 ** 38 + 1
    FLOAT_HIGH_RANGE = 2 ** 31 - 1
    FLOAT_LOW_RANGE = -2 ** 31
    DOUBLE_HIGH_RANGE = 2 ** 63 - 1
    DOUBLE_LOW_RANGE = -2 ** 63

    def __init__(self, primary_key=CassandraTypes.UUID, key_format="{}_var"):
        self.primary_key = primary_key
        self.key_format = key_format
        self.items_count = 0
        self.reset()

    @staticmethod
    def _random_string(string_size=None):
        string_size = string_size or 30
        return "".join(random.choices(string.printable, k=string_size))

    @staticmethod
    def _int_number(low, high):
        return random.randint(low, high)

    @staticmethod
    def _float_number(low, high):
        return random.uniform(low, high)

    @staticmethod
    def get_mode_name(mode):
        if not isinstance(mode, CassandraTypes):
            raise TypeError(f"The mode variable should be '{CassandraTypes.__name__}' class")
        if mode == CassandraTypes.ASCII:
            return "ascii"
        elif mode == CassandraTypes.BIGINT:
            return "64bit_int"
        elif mode == CassandraTypes.NONE:
            return "none"
        elif mode == CassandraTypes.STRING:
            return "string"
        elif mode == CassandraTypes.BINARY:
            return "binary"
        elif mode == CassandraTypes.LIST:
            return "list"
        elif mode == CassandraTypes.DICT:
            return "dict"

    def create_random_ascii_item(self, string_size=None):
        self.items_count += 1
        return "".join(str(ord(char)) for char in self._random_string(string_size=string_size))

    def create_bigint_item(self):
        self.items_count += 1
        return self._int_number(low=self.BIG_INT_LOW_RANGE, high=self.BIG_INT_HIGH_RANGE)

    def create_blob_item(self):
        self.items_count += 1
        return self._random_string(string_size=random.randint(10, 20)).encode()

    def create_bool_item(self):
        self.items_count += 1
        return bool(random.randint(0, 1))

    def create_date_item(self):
        self.items_count += 1
        if self.items_count / 2 == 0:
            return int(datetime.now().timestamp())
        return datetime.now().strftime("%Y-%m-%d")

    def create_decimal_item(self):
        self.items_count += 1
        return self._float_number(low=self.DECIMAL_LOW_RANGE, high=self.DECIMAL_HIGH_RANGE)

    def create_double_item(self):
        self.items_count += 1
        return self._float_number(low=self.DOUBLE_LOW_RANGE, high=self.DOUBLE_HIGH_RANGE)

    def create_float_item(self):
        self.items_count += 1
        return self._float_number(low=self.FLOAT_LOW_RANGE, high=self.FLOAT_HIGH_RANGE)

    def create_ip_address(self):
        self.items_count += 1
        if self.items_count / 2 == 0:
            return ".".join([str(random.randint(0, 255)) for _ in range(4)])
        return ":".join([hex(random.randint(2 ** 16, 2 ** 17))[-4:] for _ in range(8)])

    def create_int_item(self):
        self.items_count += 1
        return self._int_number(low=self.INT_LOW_RANGE, high=self.INT_HIGH_RANGE)

    def create_small_int_item(self):
        self.items_count += 1
        return self._int_number(low=self.SMALL_INT_LOW_RANGE, high=self.SMALL_INT_HIGH_RANGE)

    def create_text_item(self):
        self.items_count += 1
        return self._random_string(string_size=random.randint(10, 100))

    def create_time_item(self):
        self.items_count += 1
        return datetime.now().strftime("%H:%M:%S.%s")

    def create_timestamp_item(self):
        self.items_count += 1
        return datetime.now()

    def create_uuid1_item(self):
        self.items_count += 1
        return uuid.uuid1()

    def create_tiny_int_item(self):
        self.items_count += 1
        return self._int_number(low=self.TINY_INT_LOW_RANGE, high=self.TINY_INT_HIGH_RANGE)

    def create_uuid_item(self):
        self.items_count += 1
        return uuid.uuid4()

    def create_varchar_item(self):
        self.items_count += 1
        return self._random_string(string_size=random.randint(10, self.SMALL_INT_HIGH_RANGE))

    def create_multiple_items(self, mode, num_of_items):
        if mode == CassandraTypes.ASCII:
            return [self.create_random_ascii_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.BIGINT:
            return [self.create_bigint_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.BLOB:
            return [self.create_blob_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.BOOLEAN:
            return [self.create_bool_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.DATE:
            return [self.create_date_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.DOUBLE:
            return [self.create_double_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.DECIMAL:
            return [self.create_decimal_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.FLOAT:
            return [self.create_float_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.INET:
            return [self.create_ip_address() for _ in range(num_of_items)]
        elif mode == CassandraTypes.INT:
            return [self.create_int_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.SMALLINT:
            return [self.create_small_int_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.TEXT:
            return [self.create_text_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.TIME:
            return [self.create_time_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.TIMESTAMP:
            return [self.create_timestamp_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.TIMEUUID:
            return [self.create_uuid1_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.TINYINT:
            return [self.create_tiny_int_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.UUID:
            return [self.create_uuid_item() for _ in range(num_of_items)]
        elif mode == CassandraTypes.VARCHAR:
            return [self.create_varchar_item() for _ in range(num_of_items)]
        raise TypeError(f"The following type '{mode}' not supported")

    def create_multiple_lines(self, num_of_lines, modes):
        result = []
        for _ in range(num_of_lines):
            line = []
            for mode in modes:
                line.append(self.create_multiple_items(mode=mode, num_of_items=1)[0])
            result.append(line)
        return result

    def reset(self):
        self.items_count = 0
        random.seed(10)
