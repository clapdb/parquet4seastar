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

#include <unistd.h>

#include <parquet4seastar/column_chunk_reader.hh>
#include <parquet4seastar/column_chunk_writer.hh>
#include <parquet4seastar/file_reader.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>

namespace parquet4seastar {

constexpr bytes_view operator""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

constexpr std::string_view test_file_prefix = "/tmp/parquet4seastar_column_chunk_reader_test_";

// Helper to create unique test file name for each test
static std::string make_test_file(const char* name) {
    return std::string(test_file_prefix) + name + ".bin";
}

// Test reading a column where all values are null (def_level=0)
SEASTAR_TEST_CASE(read_all_null_column) {
    return seastar::async([] {
        std::string test_file = make_test_file("all_null");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        // Write - max def level is 1, rep level is 0
        // def_level=0 means null, def_level=1 means non-null
        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{1, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // Write all nulls by using def_level=0
        for (int i = 0; i < 10; ++i) {
            w.put(0, 0, 0);  // def=0 means null, value is ignored
        }
        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        BOOST_CHECK_EQUAL(cmd->num_values, 10);

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 1, 0, std::nullopt};

        constexpr size_t n_levels = 10;
        int32_t def[n_levels];
        int32_t rep[n_levels];
        int32_t val[n_levels];

        size_t n_read = r.read_batch(n_levels, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, n_levels);

        // All def levels should be 0 (null)
        for (size_t i = 0; i < n_levels; ++i) {
            BOOST_CHECK_EQUAL(def[i], 0);
            BOOST_CHECK_EQUAL(rep[i], 0);
        }

        // No more data
        n_read = r.read_batch(n_levels, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, 0u);
    });
}

// Test reading a column where all values are non-null
SEASTAR_TEST_CASE(read_all_non_null_column) {
    return seastar::async([] {
        std::string test_file = make_test_file("all_non_null");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{1, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // Write all non-nulls with def_level=1
        std::vector<int32_t> expected_values;
        for (int i = 0; i < 10; ++i) {
            expected_values.push_back(i * 100);
            w.put(1, 0, i * 100);
        }
        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        BOOST_CHECK_EQUAL(cmd->num_values, 10);

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 1, 0, std::nullopt};

        constexpr size_t n_levels = 10;
        int32_t def[n_levels];
        int32_t rep[n_levels];
        int32_t val[n_levels];

        size_t n_read = r.read_batch(n_levels, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, n_levels);

        // All def levels should be 1 (non-null)
        for (size_t i = 0; i < n_levels; ++i) {
            BOOST_CHECK_EQUAL(def[i], 1);
            BOOST_CHECK_EQUAL(rep[i], 0);
            BOOST_CHECK_EQUAL(val[i], expected_values[i]);
        }
    });
}

// Test reading a column with mixed null/non-null values
SEASTAR_TEST_CASE(read_mixed_null_column) {
    return seastar::async([] {
        std::string test_file = make_test_file("mixed_null");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{1, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // Write mixed: non-null, non-null, null, non-null, null
        w.put(1, 0, 100);  // non-null
        w.put(1, 0, 200);  // non-null
        w.put(0, 0, 0);    // null
        w.put(1, 0, 300);  // non-null
        w.put(0, 0, 0);    // null

        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        BOOST_CHECK_EQUAL(cmd->num_values, 5);

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 1, 0, std::nullopt};

        constexpr size_t n_levels = 5;
        int32_t def[n_levels];
        int32_t rep[n_levels];
        int32_t val[3];  // Only 3 non-null values

        size_t n_read = r.read_batch(n_levels, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, n_levels);

        // Check def levels
        int32_t expected_def[] = {1, 1, 0, 1, 0};
        BOOST_CHECK_EQUAL_COLLECTIONS(def, def + n_levels, expected_def, expected_def + n_levels);

        // Check values (only non-null)
        int32_t expected_val[] = {100, 200, 300};
        BOOST_CHECK_EQUAL_COLLECTIONS(val, val + 3, expected_val, expected_val + 3);
    });
}

// Test reading a simple list with repetition levels
SEASTAR_TEST_CASE(read_simple_list) {
    return seastar::async([] {
        std::string test_file = make_test_file("simple_list");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        // def_level=2 (list exists + element exists), rep_level=1 (list item repetition)
        column_chunk_writer<INT32> w{2, 1, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // First record: list [1, 2, 3]
        w.put(2, 0, 1);  // rep=0 starts new record, def=2 means value exists
        w.put(2, 1, 2);  // rep=1 continues list
        w.put(2, 1, 3);  // rep=1 continues list

        // Second record: list [10, 20]
        w.put(2, 0, 10);  // rep=0 starts new record
        w.put(2, 1, 20);  // rep=1 continues list

        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        BOOST_CHECK_EQUAL(cmd->num_values, 5);

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 2, 1, std::nullopt};

        constexpr size_t n_levels = 5;
        int32_t def[n_levels];
        int32_t rep[n_levels];
        int32_t val[n_levels];

        size_t n_read = r.read_batch(n_levels, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, n_levels);

        int32_t expected_def[] = {2, 2, 2, 2, 2};
        int32_t expected_rep[] = {0, 1, 1, 0, 1};
        int32_t expected_val[] = {1, 2, 3, 10, 20};

        BOOST_CHECK_EQUAL_COLLECTIONS(def, def + n_levels, expected_def, expected_def + n_levels);
        BOOST_CHECK_EQUAL_COLLECTIONS(rep, rep + n_levels, expected_rep, expected_rep + n_levels);
        BOOST_CHECK_EQUAL_COLLECTIONS(val, val + n_levels, expected_val, expected_val + n_levels);
    });
}

// Test reading nested optional fields
SEASTAR_TEST_CASE(read_nested_optional) {
    return seastar::async([] {
        std::string test_file = make_test_file("nested_optional");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        // Nested optional: optional<optional<int>>
        // def_level: 0=outer null, 1=inner null, 2=value exists
        column_chunk_writer<INT32> w{2, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        w.put(2, 0, 42);   // Both present, value=42
        w.put(1, 0, 0);    // Outer present, inner null
        w.put(0, 0, 0);    // Outer null
        w.put(2, 0, 100);  // Both present, value=100

        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 2, 0, std::nullopt};

        constexpr size_t n_levels = 4;
        int32_t def[n_levels];
        int32_t rep[n_levels];
        int32_t val[2];  // Only 2 non-null values

        size_t n_read = r.read_batch(n_levels, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, n_levels);

        int32_t expected_def[] = {2, 1, 0, 2};
        BOOST_CHECK_EQUAL_COLLECTIONS(def, def + n_levels, expected_def, expected_def + n_levels);

        int32_t expected_val[] = {42, 100};
        BOOST_CHECK_EQUAL_COLLECTIONS(val, val + 2, expected_val, expected_val + 2);
    });
}

// Test batch boundary handling - read in small batches
SEASTAR_TEST_CASE(read_batch_boundary) {
    return seastar::async([] {
        std::string test_file = make_test_file("batch_boundary");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{0, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // Write 100 values
        std::vector<int32_t> expected_values;
        for (int i = 0; i < 100; ++i) {
            expected_values.push_back(i);
            w.put(0, 0, i);
        }
        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        // Read in small batches
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 0, 0, std::nullopt};

        std::vector<int32_t> all_values;
        int32_t def[10];
        int32_t rep[10];
        int32_t val[10];

        while (true) {
            size_t n_read = r.read_batch(10, def, rep, val).get();
            if (n_read == 0) {
                break;
            }
            for (size_t i = 0; i < n_read; ++i) {
                all_values.push_back(val[i]);
            }
        }

        BOOST_CHECK_EQUAL(all_values.size(), expected_values.size());
        BOOST_CHECK_EQUAL_COLLECTIONS(all_values.begin(), all_values.end(), expected_values.begin(),
                                      expected_values.end());
    });
}

// Test reading across multiple pages
SEASTAR_TEST_CASE(read_multiple_pages) {
    return seastar::async([] {
        std::string test_file = make_test_file("multiple_pages");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{0, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // Write to first page
        std::vector<int32_t> expected_values;
        for (int i = 0; i < 50; ++i) {
            expected_values.push_back(i);
            w.put(0, 0, i);
        }
        w.flush_page();

        // Write to second page
        for (int i = 50; i < 100; ++i) {
            expected_values.push_back(i);
            w.put(0, 0, i);
        }
        w.flush_page();

        // Write to third page
        for (int i = 100; i < 120; ++i) {
            expected_values.push_back(i);
            w.put(0, 0, i);
        }

        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        // Read all at once
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 0, 0, std::nullopt};

        std::vector<int32_t> all_values;
        int32_t def[200];
        int32_t rep[200];
        int32_t val[200];

        while (true) {
            size_t n_read = r.read_batch(200, def, rep, val).get();
            if (n_read == 0) {
                break;
            }
            for (size_t i = 0; i < n_read; ++i) {
                all_values.push_back(val[i]);
            }
        }

        BOOST_CHECK_EQUAL(all_values.size(), expected_values.size());
        BOOST_CHECK_EQUAL_COLLECTIONS(all_values.begin(), all_values.end(), expected_values.begin(),
                                      expected_values.end());
    });
}

// Test reading with compression
SEASTAR_TEST_CASE(read_with_compression) {
    return seastar::async([] {
        std::string test_file = make_test_file("compressed");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{0, 0, make_value_encoder<INT32>(format::Encoding::PLAIN),
                                     compressor::make(format::CompressionCodec::SNAPPY)};

        std::vector<int32_t> expected_values;
        for (int i = 0; i < 1000; ++i) {
            expected_values.push_back(i * 7);
            w.put(0, 0, i * 7);
        }
        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::SNAPPY, 0, 0, std::nullopt};

        std::vector<int32_t> all_values;
        int32_t def[1000];
        int32_t rep[1000];
        int32_t val[1000];

        while (true) {
            size_t n_read = r.read_batch(1000, def, rep, val).get();
            if (n_read == 0) {
                break;
            }
            for (size_t i = 0; i < n_read; ++i) {
                all_values.push_back(val[i]);
            }
        }

        BOOST_CHECK_EQUAL(all_values.size(), expected_values.size());
        BOOST_CHECK_EQUAL_COLLECTIONS(all_values.begin(), all_values.end(), expected_values.begin(),
                                      expected_values.end());
    });
}

// Test with dictionary encoding
SEASTAR_TEST_CASE(read_dictionary_encoded) {
    return seastar::async([] {
        std::string test_file = make_test_file("dictionary");
        seastar::file output_file =
          seastar::open_file_dma(test_file.data(),
                                 seastar::open_flags::wo | seastar::open_flags::truncate | seastar::open_flags::create)
            .get();

        seastar::output_stream<char> output = seastar::make_file_output_stream(output_file).get();
        constexpr format::Type::type INT32 = format::Type::INT32;
        column_chunk_writer<INT32> w{0, 0, make_value_encoder<INT32>(format::Encoding::RLE_DICTIONARY),
                                     compressor::make(format::CompressionCodec::UNCOMPRESSED)};

        // Write values with limited cardinality (good for dictionary encoding)
        std::vector<int32_t> expected_values = {10, 20, 10, 30, 20, 10, 30, 30, 20, 10};
        for (auto v : expected_values) {
            w.put(0, 0, v);
        }
        seastar::lw_shared_ptr<format::ColumnMetaData> cmd = w.flush_chunk(output).get();
        output.flush().get();
        output.close().get();

        // Read
        seastar::file input_file = seastar::open_file_dma(test_file.data(), seastar::open_flags::ro).get();

        column_chunk_reader<INT32> r{page_reader{SeastarFile(input_file).make_peekable_stream()},
                                     format::CompressionCodec::UNCOMPRESSED, 0, 0, std::nullopt};

        int32_t def[10];
        int32_t rep[10];
        int32_t val[10];

        size_t n_read = r.read_batch(10, def, rep, val).get();
        BOOST_CHECK_EQUAL(n_read, 10u);

        std::vector<int32_t> decoded(val, val + n_read);
        BOOST_CHECK_EQUAL_COLLECTIONS(decoded.begin(), decoded.end(), expected_values.begin(), expected_values.end());
    });
}

}  // namespace parquet4seastar
