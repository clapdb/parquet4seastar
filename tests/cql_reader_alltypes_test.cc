/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2020 ScyllaDB
 */

#include <map>
#include <parquet4seastar/cql_reader.hh>
#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/reader_schema.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <sstream>

namespace parquet4seastar {

const char* basic_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "bool_ct" boolean, "bool_lt" boolean, "int8_ct" tinyint, "int8_lt" tinyint, "int16_ct" smallint, "int16_lt" smallint, "int32_ct" int, "int32_lt" int, "int64_ct" bigint, "int64_lt" bigint, "int96_ct" varint, "uint8_ct" smallint, "uint8_lt" smallint, "uint16_ct" int, "uint16_lt" int, "uint32_ct" bigint, "uint32_lt" bigint, "uint64_ct" varint, "uint64_lt" varint, "float_ct" float, "float_lt" float, "double_ct" double, "double_lt" double);
INSERT INTO "parquet"("row_number", "bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt") VALUES(0, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, 255, 255, 65535, 65535, 4294967295, 4294967295, 18446744073709551615, 18446744073709551615, -1.100000e+00, -1.100000e+00, -1.111111e+00, -1.111111e+00);
INSERT INTO "parquet"("row_number", "bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt") VALUES(1, true, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00);
INSERT INTO "parquet"("row_number", "bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt") VALUES(2, false, false, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.100000e+00, 1.100000e+00, 1.111111e+00, 1.111111e+00);
)###";

const char* date_time_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "date_ct" int, "date_lt" int, "time_millis_ct" time, "time_utc_millis_lt" time, "time_nonutc_millis_lt" time, "time_micros_ct" time, "time_utc_micros_lt" time, "time_nonutc_micros_lt" time, "time_utc_nanos" time, "time_nonutc_nanos" time);
INSERT INTO "parquet"("row_number", "date_ct", "date_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos") VALUES(0, -1, -1, '00:00:00.000', '00:00:00.000', '00:00:00.000', '00:00:00.000000', '00:00:00.000000', '00:00:00.000000', '00:00:00.000000000', '00:00:00.000000000');
INSERT INTO "parquet"("row_number", "date_ct", "date_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos") VALUES(1, 0, 0, '01:01:01.000', '01:01:01.000', '01:01:01.000', '01:01:01.000000', '01:01:01.000000', '01:01:01.000000', '01:01:01.000000000', '01:01:01.000000000');
INSERT INTO "parquet"("row_number", "date_ct", "date_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos") VALUES(2, 1, 1, '02:02:02.000', '02:02:02.000', '02:02:02.000', '02:02:02.000000', '02:02:02.000000', '02:02:02.000000', '02:02:02.000000000', '02:02:02.000000000');
)###";

const char* timestamp_interval_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "timestamp_millis_ct" timestamp, "timestamp_utc_millis_lt" timestamp, "timestamp_nonutc_millis_lt" timestamp, "timestamp_micros_ct" bigint, "timestamp_utc_micros_lt" bigint, "timestamp_nonutc_micros_lt" bigint, "timestamp_utc_nanos" bigint, "timestamp_nonutc_nanos" bigint, "interval_ct" duration, "interval_lt" duration);
INSERT INTO "parquet"("row_number", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt") VALUES(0, -1, -1, -1, -1, -1, -1, -1, -1, 0mo0d0ms, 0mo0d0ms);
INSERT INTO "parquet"("row_number", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt") VALUES(1, 0, 0, 0, 0, 0, 0, 0, 0, 1mo1d1ms, 1mo1d1ms);
INSERT INTO "parquet"("row_number", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt") VALUES(2, 1, 1, 1, 1, 1, 1, 1, 1, 2mo2d2ms, 2mo2d2ms);
)###";

const char* decimal_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "decimal_int32_ct" decimal, "decimal_int32_lt" decimal, "decimal_int64_ct" decimal, "decimal_int64_lt" decimal, "decimal_byte_array_ct" decimal, "decimal_byte_array_lt" decimal, "decimal_flba_ct" decimal, "decimal_flba_lt" decimal);
INSERT INTO "parquet"("row_number", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt") VALUES(0, -1e-5, -1e-5, -1e-10, -1e-10, -1e-2, -1e-2, -1e-5, -1e-5);
INSERT INTO "parquet"("row_number", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt") VALUES(1, 0e-5, 0e-5, 0e-10, 0e-10, 0e-2, 0e-2, 0e-5, 0e-5);
INSERT INTO "parquet"("row_number", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt") VALUES(2, 1e-5, 1e-5, 1e-10, 1e-10, 1e-2, 1e-2, 1e-5, 1e-5);
)###";

const char* byte_array_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "utf8" text, "string" text, "10_byte_array_ct" blob, "10_byte_array_lt" blob, "enum_ct" text, "enum_lt" text, "json_ct" text, "json_lt" text, "bson_ct" blob, "bson_lt" blob, "uuid" uuid);
INSERT INTO "parquet"("row_number", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "enum_ct", "enum_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid") VALUES(0, 'parquet00/', 'parquet00/', 0xFFFFFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFFFFFF, 'ENUM   000', 'ENUM   000', '{"key":"value"}', '{"key":"value"}', 0x42534F4E, 0x42534F4E, FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF);
INSERT INTO "parquet"("row_number", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "enum_ct", "enum_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid") VALUES(1, 'parquet000', 'parquet000', 0x00000000000000000000, 0x00000000000000000000, 'ENUM   001', 'ENUM   001', '{"key":"value"}', '{"key":"value"}', 0x42534F4E, 0x42534F4E, 00000000-0000-0000-0000-000000000000);
INSERT INTO "parquet"("row_number", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "enum_ct", "enum_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid") VALUES(2, 'parquet001', 'parquet001', 0x01010101010101010101, 0x01010101010101010101, 'ENUM   002', 'ENUM   002', '{"key":"value"}', '{"key":"value"}', 0x42534F4E, 0x42534F4E, 01000000-0100-0000-0100-000001000000);
)###";

const char* collections_cql = R"###(
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "optional_uint32" bigint, "twice_repeated_uint16" frozen<list<int>>, "optional_undefined_null" int, "map_int32_int32" frozen<map<int, int>>, "map_key_value_bool_bool" frozen<map<boolean, boolean>>, "map_logical" frozen<map<int, int>>, "list_float" frozen<list<float>>, "list_double" frozen<list<double>>);
INSERT INTO "parquet"("row_number", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32", "map_key_value_bool_bool", "map_logical", "list_float", "list_double") VALUES(0, 4294967295, [0, 1], null, {-1: -1, 0: 0}, {false: false, false: false}, {-1: -1, 0: 0}, [-1.100000e+00, 0.000000e+00], [-1.111110e+00, 0.000000e+00]);
INSERT INTO "parquet"("row_number", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32", "map_key_value_bool_bool", "map_logical", "list_float", "list_double") VALUES(1, null, [2, 3], null, {0: 0, 1: 1}, {true: true, false: false}, {0: 0, 1: 1}, [0.000000e+00, 1.100000e+00], [0.000000e+00, 1.111110e+00]);
INSERT INTO "parquet"("row_number", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32", "map_key_value_bool_bool", "map_logical", "list_float", "list_double") VALUES(2, 1, [4, 5], null, {1: 1, 2: 2}, {false: false, true: true}, {1: 1, 2: 2}, [1.100000e+00, 2.200000e+00], [1.111110e+00, 2.222220e+00]);
)###";

//     optional_uint32 twice_repeated_uint16 optional_undefined_null  \
    // 0     4.294967e+09                [0, 1]                    None
// 1              NaN                [2, 3]                    None
// 2     1.000000e+00                [4, 5]                    None
//
//       map_int32_int32           map_key_value_bool_bool         map_logical  \
    // 0  [(-1, -1), (0, 0)]  [(False, False), (False, False)]  [(-1, -1), (0, 0)]
// 1    [(0, 0), (1, 1)]    [(True, True), (False, False)]    [(0, 0), (1, 1)]
// 2    [(1, 1), (2, 2)]    [(False, False), (True, True)]    [(1, 1), (2, 2)]
//
//     list_float                             list_double
// 0  [-1.1, 0.0]               [-1.111109972000122, 0.0]
// 1   [0.0, 1.1]                [0.0, 1.111109972000122]
// 2   [1.1, 2.2]  [1.111109972000122, 2.222219944000244]
SEASTAR_TEST_CASE(parquet_to_cql) {
    return seastar::async([] {
        // FIXME: work_dir in cmake not work;
        std::string path = "/home/moyi/tmp/tmp.Up1Hmh0aGD/tests/test_data/alltypes/";
        std::string suffix = ".uncompressed.plain.parquet";
        std::vector<std::pair<std::string, std::string>> test_cases = {{"basic" + suffix, basic_cql},  //
                                                                       {"collections" + suffix, collections_cql},
                                                                       {"decimal" + suffix, decimal_cql},
                                                                       {"other" + suffix, byte_array_cql},
                                                                       {"time" + suffix, date_time_cql},
                                                                       {"timestamp" + suffix, timestamp_interval_cql}};
        for (const auto& [filename, output] : test_cases) {
            std::stringstream ss;
            ss << '\n';
            auto reader = file_reader::open(path + filename).get0();
            cql::parquet_to_cql(reader, "parquet", "row_number", ss).get();
            BOOST_CHECK_EQUAL(ss.str(), output);
        }
    });
}

}  // namespace parquet4seastar
