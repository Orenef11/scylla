# -*- coding: utf-8 -*-
# Copyright 2020 ScyllaDB
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
import random
from operator import itemgetter

import pytest

from util import CassandraDataGenerator, CassandraTypes, unique_name


class TestInOperator:
    PRIMARY_KEY = CassandraTypes.UUID
    KEY_FORMAT = '{}_var'
    TABLE_COLUMNS = [CassandraTypes.ASCII, CassandraTypes.BIGINT, CassandraTypes.BLOB, CassandraTypes.BOOLEAN,
                     CassandraTypes.DATE, CassandraTypes.DOUBLE, CassandraTypes.DECIMAL, CassandraTypes.FLOAT,
                     CassandraTypes.INET, CassandraTypes.INT, CassandraTypes.SMALLINT, CassandraTypes.TEXT,
                     CassandraTypes.TIME, CassandraTypes.TIMESTAMP, CassandraTypes.TIMEUUID, CassandraTypes.TINYINT,
                     CassandraTypes.UUID, CassandraTypes.VARCHAR]
    PRIMARY_KEYS = [PRIMARY_KEY, CassandraTypes.TEXT]

    @pytest.fixture(scope="class")
    def data_generator(self, cql, test_keyspace):
        return CassandraDataGenerator(primary_key=self.PRIMARY_KEY, key_format=self.KEY_FORMAT)

    @classmethod
    @pytest.fixture(scope="class", autouse=True)
    def in_oper_table(cls, cql, test_keyspace, data_generator):
        table_columns = ", ".join(f'{cls.KEY_FORMAT.format(col.value)} {col.value}' for col in cls.TABLE_COLUMNS)
        table_name = test_keyspace + "." + unique_name()
        cql.execute(f'CREATE TABLE {table_name} ({table_columns}, PRIMARY KEY ('
                    f'{", ".join(cls.KEY_FORMAT.format(primary_key.value) for primary_key in cls.PRIMARY_KEYS)}))')
        yield table_name

    def test_in_operator_format(self, cql, data_generator, in_oper_table):
        in_query_values = []
        column_types = ", ".join([self.KEY_FORMAT.format(column.value) for column in self.TABLE_COLUMNS])
        rows = data_generator.create_multiple_lines(num_of_lines=random.randint(5, 100), modes=CassandraTypes)
        insert_query = cql.prepare(
            f"INSERT INTO {in_oper_table} ({column_types}) VALUES ({', '.join(len(CassandraTypes) * '?')});")
        for row in rows:
            cql.execute(insert_query, row)

        in_query_format = " AND ".join(f"{key} IN (?, ?)" for key in column_types.split(", "))
        in_query = cql.prepare(f"SELECT * FROM {in_oper_table} where {in_query_format} ALLOW FILTERING;")
        primary_keys_indexes = [self.TABLE_COLUMNS.index(primary_key) for primary_key in self.PRIMARY_KEYS]
        primary_keys = [self.KEY_FORMAT.format(column.value) for column in self.PRIMARY_KEYS]

        previous_row = rows[0]
        for row in rows[1:]:
            in_query_values.clear()
            expected_rows = [previous_row, row]
            for key1, key2 in zip(previous_row[: len(self.TABLE_COLUMNS)], row[: len(self.TABLE_COLUMNS)]):
                in_query_values.append(key1)
                in_query_values.append(key2)
            in_query_result = cql.execute(in_query, in_query_values).all()
            assert len(in_query_result) == len(expected_rows)
            expected_primary_keys = set(itemgetter(*primary_keys_indexes)(_row) for _row in [previous_row, row])
            in_query_result_primary_keys = set(
                tuple(getattr(_row, primary_key) for primary_key in primary_keys) for _row in in_query_result)
            assert in_query_result_primary_keys == expected_primary_keys
            previous_row = row
