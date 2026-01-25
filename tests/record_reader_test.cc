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

#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/file_writer.hh>
#include <parquet4seastar/record_reader.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <sstream>
#include <vector>

namespace parquet4seastar {

constexpr bytes_view operator""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

constexpr std::string_view test_file_prefix = "/tmp/parquet4seastar_record_reader_test_";

static std::string make_test_file(const char* name) { return std::string(test_file_prefix) + name + ".parquet"; }

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

// A simple consumer that records events for verification
class test_consumer
{
   public:
    std::vector<std::string> events;

    void start_record() { events.push_back("start_record"); }
    void end_record() { events.push_back("end_record"); }
    void start_column(const std::string& name) { events.push_back("start_column:" + name); }
    void start_struct() { events.push_back("start_struct"); }
    void end_struct() { events.push_back("end_struct"); }
    void start_list() { events.push_back("start_list"); }
    void end_list() { events.push_back("end_list"); }
    void separate_list_values() { events.push_back("separate_list_values"); }
    void start_map() { events.push_back("start_map"); }
    void end_map() { events.push_back("end_map"); }
    void separate_map_values() { events.push_back("separate_map_values"); }
    void separate_key_value() { events.push_back("separate_key_value"); }
    void append_null() { events.push_back("null"); }

    template <typename LogicalType, typename Value>
    void append_value(const LogicalType&, Value&& val) {
        std::stringstream ss;
        if constexpr (std::is_same_v<std::decay_t<Value>, int32_t>) {
            ss << "int32:" << val;
        } else if constexpr (std::is_same_v<std::decay_t<Value>, int64_t>) {
            ss << "int64:" << val;
        } else if constexpr (std::is_same_v<std::decay_t<Value>, float>) {
            ss << "float:" << val;
        } else if constexpr (std::is_same_v<std::decay_t<Value>, double>) {
            ss << "double:" << val;
        } else if constexpr (std::is_same_v<std::decay_t<Value>, uint8_t>) {
            ss << "bool:" << static_cast<int>(val);
        } else if constexpr (std::is_same_v<std::decay_t<Value>, seastar::temporary_buffer<uint8_t>>) {
            ss << "bytes:" << std::string(reinterpret_cast<const char*>(val.get()), val.size());
        } else {
            ss << "unknown";
        }
        events.push_back(ss.str());
    }

    void clear() { events.clear(); }

    std::string join(const std::string& sep = ",") const {
        std::string result;
        for (size_t i = 0; i < events.size(); ++i) {
            if (i > 0) {
                result += sep;
            }
            result += events[i];
        }
        return result;
    }
};

// Test reading primitive fields
SEASTAR_TEST_CASE(read_primitive_field) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("primitive");

        // Write
        schema writer_schema = schema{vec<node>(primitive_node{"int_field",
                                                               false,
                                                               logical_type::INT32{},
                                                               {},
                                                               format::Encoding::PLAIN,
                                                               format::CompressionCodec::UNCOMPRESSED})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& int_field = fw->column<format::Type::INT32>(0);
        int_field.put(0, 0, 42);
        int_field.put(0, 0, 100);
        int_field.put(0, 0, -1);

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_all(consumer).get();

        // Verify the events
        BOOST_CHECK_GE(consumer.events.size(), 3u);

        // Check for correct values
        bool found_42 = false;
        bool found_100 = false;
        bool found_minus1 = false;
        for (const auto& event : consumer.events) {
            if (event == "int32:42") {
                found_42 = true;
            }
            if (event == "int32:100") {
                found_100 = true;
            }
            if (event == "int32:-1") {
                found_minus1 = true;
            }
        }
        BOOST_CHECK(found_42);
        BOOST_CHECK(found_100);
        BOOST_CHECK(found_minus1);
    });
}

// Test reading optional fields (with nulls)
SEASTAR_TEST_CASE(read_optional_field) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("optional");

        // Write - optional int field
        schema writer_schema = schema{vec<node>(primitive_node{"opt_int",
                                                               true,  // optional
                                                               logical_type::INT32{},
                                                               {},
                                                               format::Encoding::PLAIN,
                                                               format::CompressionCodec::UNCOMPRESSED})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& opt_int = fw->column<format::Type::INT32>(0);
        opt_int.put(1, 0, 10);    // non-null value
        opt_int.put(0, 0, 0);     // null
        opt_int.put(1, 0, 20);    // non-null value
        opt_int.put(0, 0, 0);     // null
        opt_int.put(0, 0, 0);     // null
        opt_int.put(1, 0, 30);    // non-null value

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_all(consumer).get();

        // Count nulls and values
        int null_count = 0;
        int value_count = 0;
        for (const auto& event : consumer.events) {
            if (event == "null") {
                null_count++;
            }
            if (event.find("int32:") == 0) {
                value_count++;
            }
        }
        BOOST_CHECK_EQUAL(null_count, 3);
        BOOST_CHECK_EQUAL(value_count, 3);
    });
}

// Test reading struct fields
SEASTAR_TEST_CASE(read_struct_field) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("struct");

        // Write - struct with two fields
        schema writer_schema =
          schema{vec<node>(struct_node{"my_struct",
                                       false,
                                       vec<node>(primitive_node{"field_a",
                                                                false,
                                                                logical_type::INT32{},
                                                                {},
                                                                format::Encoding::PLAIN,
                                                                format::CompressionCodec::UNCOMPRESSED},
                                                 primitive_node{"field_b",
                                                                false,
                                                                logical_type::INT64{},
                                                                {},
                                                                format::Encoding::PLAIN,
                                                                format::CompressionCodec::UNCOMPRESSED})})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& field_a = fw->column<format::Type::INT32>(0);
        auto& field_b = fw->column<format::Type::INT64>(1);

        // Write two records
        field_a.put(0, 0, 1);
        field_b.put(0, 0, 100);

        field_a.put(0, 0, 2);
        field_b.put(0, 0, 200);

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_all(consumer).get();

        // Verify struct events
        int start_struct_count = 0;
        int end_struct_count = 0;
        for (const auto& event : consumer.events) {
            if (event == "start_struct") {
                start_struct_count++;
            }
            if (event == "end_struct") {
                end_struct_count++;
            }
        }
        BOOST_CHECK_EQUAL(start_struct_count, 2);
        BOOST_CHECK_EQUAL(end_struct_count, 2);
    });
}

// Test reading list fields
SEASTAR_TEST_CASE(read_list_field) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("list");

        // Write - list of ints
        schema writer_schema =
          schema{vec<node>(list_node{"my_list",
                                     false,
                                     box<node>(primitive_node{"item",
                                                              false,
                                                              logical_type::INT32{},
                                                              {},
                                                              format::Encoding::PLAIN,
                                                              format::CompressionCodec::UNCOMPRESSED})})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& list_item = fw->column<format::Type::INT32>(0);

        // First record: list [1, 2, 3]
        list_item.put(2, 0, 1);   // rep=0 starts new record, def=2 means value exists
        list_item.put(2, 1, 2);   // rep=1 continues list
        list_item.put(2, 1, 3);   // rep=1 continues list

        // Second record: list [10, 20]
        list_item.put(2, 0, 10);  // rep=0 starts new record
        list_item.put(2, 1, 20);  // rep=1 continues list

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_all(consumer).get();

        // Verify list events
        int start_list_count = 0;
        int end_list_count = 0;
        int separate_values_count = 0;
        for (const auto& event : consumer.events) {
            if (event == "start_list") {
                start_list_count++;
            }
            if (event == "end_list") {
                end_list_count++;
            }
            if (event == "separate_list_values") {
                separate_values_count++;
            }
        }
        BOOST_CHECK_EQUAL(start_list_count, 2);   // Two records
        BOOST_CHECK_EQUAL(end_list_count, 2);
        BOOST_CHECK_EQUAL(separate_values_count, 3);  // 2 separators for first list + 1 for second
    });
}

// Test reading map fields
SEASTAR_TEST_CASE(read_map_field) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("map");

        // Write - map of string to int
        schema writer_schema = schema{vec<node>(map_node{
          "my_map",
          false,
          box<node>(primitive_node{"key",
                                   false,
                                   logical_type::STRING{},
                                   {},
                                   format::Encoding::PLAIN,
                                   format::CompressionCodec::UNCOMPRESSED}),
          box<node>(primitive_node{"value",
                                   false,
                                   logical_type::INT32{},
                                   {},
                                   format::Encoding::PLAIN,
                                   format::CompressionCodec::UNCOMPRESSED}),
        })};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& map_key = fw->column<format::Type::BYTE_ARRAY>(0);
        auto& map_value = fw->column<format::Type::INT32>(1);

        // First record: map {"a": 1, "b": 2}
        map_key.put(2, 0, "a"_bv);
        map_value.put(2, 0, 1);
        map_key.put(2, 1, "b"_bv);
        map_value.put(2, 1, 2);

        // Second record: map {"x": 100}
        map_key.put(2, 0, "x"_bv);
        map_value.put(2, 0, 100);

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_all(consumer).get();

        // Verify map events
        int start_map_count = 0;
        int end_map_count = 0;
        int separate_key_value_count = 0;
        for (const auto& event : consumer.events) {
            if (event == "start_map") {
                start_map_count++;
            }
            if (event == "end_map") {
                end_map_count++;
            }
            if (event == "separate_key_value") {
                separate_key_value_count++;
            }
        }
        BOOST_CHECK_EQUAL(start_map_count, 2);
        BOOST_CHECK_EQUAL(end_map_count, 2);
        BOOST_CHECK_EQUAL(separate_key_value_count, 3);  // 3 key-value pairs total
    });
}

// Test reading nested structures (list of structs)
SEASTAR_TEST_CASE(read_nested_structure) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("nested");

        // Write - list of optional structs
        schema writer_schema = schema{vec<node>(
          list_node{"nested_list",
                    true,
                    box<node>(struct_node{"item_struct",
                                          true,
                                          vec<node>(primitive_node{"x",
                                                                   false,
                                                                   logical_type::INT32{},
                                                                   {},
                                                                   format::Encoding::PLAIN,
                                                                   format::CompressionCodec::UNCOMPRESSED},
                                                    primitive_node{"y",
                                                                   false,
                                                                   logical_type::INT32{},
                                                                   {},
                                                                   format::Encoding::PLAIN,
                                                                   format::CompressionCodec::UNCOMPRESSED})})})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& x = fw->column<format::Type::INT32>(0);
        auto& y = fw->column<format::Type::INT32>(1);

        // Record 1: [{x:1, y:2}, {x:3, y:4}]
        x.put(4, 0, 1);
        y.put(4, 0, 2);
        x.put(4, 1, 3);
        y.put(4, 1, 4);

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_all(consumer).get();

        // Verify nested structure events
        int start_list_count = 0;
        int start_struct_count = 0;
        for (const auto& event : consumer.events) {
            if (event == "start_list") {
                start_list_count++;
            }
            if (event == "start_struct") {
                start_struct_count++;
            }
        }
        BOOST_CHECK_GE(start_list_count, 1);
        BOOST_CHECK_GE(start_struct_count, 2);  // Two structs in the list
    });
}

// Test consumer event sequence for a simple record
SEASTAR_TEST_CASE(consumer_event_sequence) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("event_seq");

        // Write - single int field
        schema writer_schema = schema{vec<node>(primitive_node{"value",
                                                               false,
                                                               logical_type::INT32{},
                                                               {},
                                                               format::Encoding::PLAIN,
                                                               format::CompressionCodec::UNCOMPRESSED})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& value = fw->column<format::Type::INT32>(0);
        value.put(0, 0, 999);

        fw->close().get();

        // Read
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        auto rr = record::record_reader::make(fr, 0).get();

        test_consumer consumer;
        rr.read_one(consumer).get();

        // Verify event sequence
        BOOST_REQUIRE(!consumer.events.empty());
        BOOST_CHECK_EQUAL(consumer.events[0], "start_record");
        BOOST_CHECK_EQUAL(consumer.events.back(), "end_record");

        // Check for the value
        bool found_value = false;
        for (const auto& event : consumer.events) {
            if (event == "int32:999") {
                found_value = true;
            }
        }
        BOOST_CHECK(found_value);
    });
}

// Test reading multiple row groups
SEASTAR_TEST_CASE(read_multiple_row_groups) {
    using namespace writer_schema;

    return seastar::async([] {
        std::string test_file = make_test_file("multi_rg");

        // Write - int field with multiple row groups
        schema writer_schema = schema{vec<node>(primitive_node{"value",
                                                               false,
                                                               logical_type::INT32{},
                                                               {},
                                                               format::Encoding::PLAIN,
                                                               format::CompressionCodec::UNCOMPRESSED})};

        seastar::open_flags flags =
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        auto file = seastar::open_file_dma(test_file, flags).get();
        auto sink = seastar::make_file_output_stream(file).get();
        auto fw = writer<seastar::output_stream<char>>::open(std::move(sink), writer_schema).get();

        auto& value = fw->column<format::Type::INT32>(0);

        // First row group
        value.put(0, 0, 1);
        value.put(0, 0, 2);
        fw->flush_row_group().get();

        // Second row group
        value.put(0, 0, 3);
        value.put(0, 0, 4);

        fw->close().get();

        // Read both row groups
        auto read_file = seastar::open_file_dma(test_file, seastar::open_flags::ro).get();
        std::unique_ptr<IReader> file_ptr = std::make_unique<SeastarFile>(SeastarFile(read_file));
        auto fr = file_reader::open(std::move(file_ptr)).get();

        // Read row group 0
        auto rr0 = record::record_reader::make(fr, 0).get();
        test_consumer consumer0;
        rr0.read_all(consumer0).get();

        // Read row group 1
        auto rr1 = record::record_reader::make(fr, 1).get();
        test_consumer consumer1;
        rr1.read_all(consumer1).get();

        // Verify both row groups have records
        int rg0_values = 0;
        int rg1_values = 0;
        for (const auto& e : consumer0.events) {
            if (e.find("int32:") == 0) {
                rg0_values++;
            }
        }
        for (const auto& e : consumer1.events) {
            if (e.find("int32:") == 0) {
                rg1_values++;
            }
        }
        BOOST_CHECK_EQUAL(rg0_values, 2);
        BOOST_CHECK_EQUAL(rg1_values, 2);
    });
}

}  // namespace parquet4seastar
