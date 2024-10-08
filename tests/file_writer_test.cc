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

#include <parquet4seastar/cql_reader.hh>
#include <parquet4seastar/file_writer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>

const std::string test_file_name = "/tmp/parquet4seastar_file_writer_test.parquet";

constexpr parquet4seastar::bytes_view operator""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

template <typename T>
std::unique_ptr<T> box(T&& x) {
    return std::make_unique<T>(std::forward<T>(x));
}

template <typename T, typename Targ>
void vec_fill(std::vector<T>& v, Targ&& arg) {
    v.push_back(std::forward<Targ>(arg));
}

template <typename T, typename Targ, typename... Targs>
void vec_fill(std::vector<T>& v, Targ&& arg, Targs&&... args) {
    v.push_back(std::forward<Targ>(arg));
    vec_fill(v, std::forward<Targs>(args)...);
}

template <typename T, typename... Targs>
std::vector<T> vec(Targs&&... args) {
    std::vector<T> v;
    vec_fill(v, std::forward<Targs>(args)...);
    return v;
}

template <typename T>
std::vector<T> vec() {
    return std::vector<T>();
}

class MemorySink
{
   public:
    std::vector<char> data;

    auto write(const char* str, size_t len) -> void {
        for (auto idx = 0; idx < len; ++idx) {
            data.push_back(str[idx]);
        }
    }
    auto flush() -> void {}
    auto close() -> void {}
};

static_assert(parquet4seastar::is_sync_sink_v<MemorySink>);

SEASTAR_TEST_CASE(full_roundtrip) {
    using namespace parquet4seastar;

    return seastar::async([] {
        // Write
        writer_schema::schema writer_schema = []() -> writer_schema::schema {
            using namespace writer_schema;
            return schema{vec<node>(
              map_node{
                "Map",
                true,
                box<node>(primitive_node{"Map key",
                                         false,
                                         logical_type::STRING{},
                                         {},
                                         format::Encoding::RLE_DICTIONARY,
                                         format::CompressionCodec::GZIP}),
                box<node>(primitive_node{"Map value",
                                         false,
                                         logical_type::INT32{},
                                         {},
                                         format::Encoding::PLAIN,
                                         format::CompressionCodec::SNAPPY}),
              },
              list_node{
                "List", true,
                box<node>(struct_node{"Struct", true,
                                      vec<node>(primitive_node{"Struct field 1", false, logical_type::FLOAT{}},
                                                primitive_node{"Struct field 2", false, logical_type::DOUBLE{}})})})};
        }();
        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = open_file_dma(test_file_name, flags).get0();
        auto sink = make_file_output_stream(file).get0();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get0();
        auto memory_fw = sync_writer<MemorySink>::open(MemorySink(), writer_schema);
        {
            auto& map_key = fw->column<format::Type::BYTE_ARRAY>(0);
            auto& map_value = fw->column<format::Type::INT32>(1);
            auto& struct_field_1 = fw->column<format::Type::FLOAT>(2);
            auto& struct_field_2 = fw->column<format::Type::DOUBLE>(3);

            map_key.put(0, 0, "1337"_bv);
            map_value.put(0, 0, 1337);
            struct_field_1.put(0, 0, 1337);
            struct_field_2.put(0, 0, 1337);

            fw->flush_row_group().get0();

            map_key.put(2, 0, "key1"_bv);
            map_value.put(2, 0, 1);
            map_key.put(2, 1, "key2"_bv);
            map_value.put(2, 1, 1);
            struct_field_1.put(2, 0, 1337);
            struct_field_2.put(2, 0, 1337);
            struct_field_1.put(3, 1, 1);
            struct_field_2.put(3, 1, 1);
        }
        {
            auto& map_key = memory_fw->column<format::Type::BYTE_ARRAY>(0);
            auto& map_value = memory_fw->column<format::Type::INT32>(1);
            auto& struct_field_1 = memory_fw->column<format::Type::FLOAT>(2);
            auto& struct_field_2 = memory_fw->column<format::Type::DOUBLE>(3);

            map_key.put(0, 0, "1337"_bv);
            map_value.put(0, 0, 1337);
            struct_field_1.put(0, 0, 1337);
            struct_field_2.put(0, 0, 1337);

            memory_fw->flush_row_group();

            map_key.put(2, 0, "key1"_bv);
            map_value.put(2, 0, 1);
            map_key.put(2, 1, "key2"_bv);
            map_value.put(2, 1, 1);
            struct_field_1.put(2, 0, 1337);
            struct_field_2.put(2, 0, 1337);
            struct_field_1.put(3, 1, 1);
            struct_field_2.put(3, 1, 1);
        }

        fw->close().get0();
        memory_fw->close();

        auto parquet_file = seastar::open_file_dma(test_file_name, seastar::open_flags::ro).get0();
        auto size = parquet_file.size().get();
        auto buffer = parquet_file.dma_read<char>(0, size).get();
        BOOST_CHECK_EQUAL(std::vector<char>(buffer.begin(), buffer.end()), memory_fw->fetch_sink().data);

        // Read
        auto seastar_file = open_file_dma(test_file_name, seastar::open_flags::ro).get();

        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(seastar_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();
        std::stringstream ss;
        ss << '\n';
        cql::parquet_to_cql(fr, "parquet", "row_number", ss).get();
        std::string output = R"###(
CREATE TYPE "parquet_udt_0" ("Struct field 1" float, "Struct field 2" double);
CREATE TABLE "parquet"("row_number" bigint PRIMARY KEY, "Map" frozen<map<text, int>>, "List" frozen<list<"parquet_udt_0">>);
INSERT INTO "parquet"("row_number", "Map", "List") VALUES(0, null, null);
INSERT INTO "parquet"("row_number", "Map", "List") VALUES(1, {'key1': 1, 'key2': 1}, [null, {"Struct field 1": 1.000000e+00, "Struct field 2": 1.000000e+00}]);
)###";
        BOOST_CHECK_EQUAL(ss.str(), output);
    });
}
