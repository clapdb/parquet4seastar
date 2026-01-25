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

// Tests for encoding optimizations to verify correctness under stress

#include <array>
#include <limits>
#include <parquet4seastar/encoding.hh>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <vector>

constexpr parquet4seastar::bytes_view operator""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

// Test delta binary packed encoder with large dataset spanning multiple blocks
// Validates the optimized flush_block() logic with reduced loop iterations
SEASTAR_TEST_CASE(delta_binary_packed_large_dataset) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT64>({});

    // Generate large dataset that spans multiple blocks (128 values per block)
    std::vector<int64_t> input;
    input.reserve(10000);

    // Pattern 1: Monotonically increasing (good delta compression)
    for (int64_t i = 0; i < 3000; ++i) {
        input.push_back(i * 100);
    }

    // Pattern 2: Large deltas with varied signs
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist(std::numeric_limits<int64_t>::min() / 2,
                                                 std::numeric_limits<int64_t>::max() / 2);
    for (int i = 0; i < 2000; ++i) {
        input.push_back(dist(rng));
    }

    // Pattern 3: Small deltas (efficient bit packing)
    int64_t base = 1000000;
    for (int i = 0; i < 2000; ++i) {
        input.push_back(base + (i % 16) - 8);
    }

    // Pattern 4: Edge cases
    input.push_back(std::numeric_limits<int64_t>::min());
    input.push_back(std::numeric_limits<int64_t>::max());
    input.push_back(0);
    input.push_back(-1);
    input.push_back(1);

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
    std::vector<int64_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

// Test delta binary packed encoder with data that triggers all miniblocks
// Validates the combined delta computation + max_delta calculation loop
SEASTAR_TEST_CASE(delta_binary_packed_miniblock_boundaries) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT32>({});

    // Create data that exactly fills one block (128 values = 4 miniblocks of 32)
    std::vector<int32_t> input;

    // Miniblock 1: Small positive deltas (bit width 2-3)
    for (int i = 0; i < 32; ++i) {
        input.push_back(i);
    }

    // Miniblock 2: Larger deltas (bit width 8-10)
    for (int i = 0; i < 32; ++i) {
        input.push_back(input.back() + (i % 2 ? 100 : -50));
    }

    // Miniblock 3: Very large deltas (bit width 16+)
    for (int i = 0; i < 32; ++i) {
        input.push_back(input.back() + (i % 2 ? 10000 : -5000));
    }

    // Miniblock 4: Mixed pattern
    for (int i = 0; i < 32; ++i) {
        input.push_back(input.back() + ((i * 7) % 13) - 6);
    }

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
    std::vector<int32_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

// Test delta length byte array decoder with large number of strings
// Validates the optimized reserve() strategy and exponential growth
SEASTAR_TEST_CASE(delta_length_byte_array_large_batch) {
    using namespace parquet4seastar;

    // Create encoder to generate test data
    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::DELTA_LENGTH_BYTE_ARRAY);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    // Generate large number of strings (> BATCH_SIZE * 4 to test exponential growth)
    std::vector<seastar::temporary_buffer<uint8_t>> input_strings;
    std::vector<bytes_view> input_views;
    input_strings.reserve(5000);
    input_views.reserve(5000);

    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> len_dist(5, 50);
    std::uniform_int_distribution<uint8_t> char_dist('a', 'z');

    for (size_t i = 0; i < 5000; ++i) {
        size_t len = len_dist(rng);
        auto buf = seastar::temporary_buffer<uint8_t>(len);
        for (size_t j = 0; j < len; ++j) {
            buf.get_write()[j] = char_dist(rng);
        }
        input_views.emplace_back(buf.get(), buf.size());
        input_strings.push_back(std::move(buf));
    }

    encoder->put_batch(input_views.data(), input_views.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_LENGTH_BYTE_ARRAY);

    std::vector<seastar::temporary_buffer<uint8_t>> decoded;
    decoded.resize(input_views.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input_views.size());
    for (size_t i = 0; i < decoded.size(); ++i) {
        BOOST_CHECK_EQUAL(decoded[i].size(), input_views[i].size());
        BOOST_CHECK(std::equal(decoded[i].begin(), decoded[i].end(),
                               input_views[i].begin(), input_views[i].end()));
    }

    return seastar::async([]() {});
}

// Test plain BYTE_ARRAY encoder with large batch
// Validates the optimized put_batch() with pre-calculated reserve()
SEASTAR_TEST_CASE(plain_byte_array_large_batch) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    // Generate many strings to stress-test the reserve optimization
    std::vector<seastar::temporary_buffer<uint8_t>> input_strings;
    std::vector<bytes_view> input_views;
    input_strings.reserve(2000);
    input_views.reserve(2000);

    // Mix of small and large strings
    for (size_t i = 0; i < 1000; ++i) {
        std::string s = "small_" + std::to_string(i);
        auto buf = seastar::temporary_buffer<uint8_t>(s.size());
        std::memcpy(buf.get_write(), s.data(), s.size());
        input_views.emplace_back(buf.get(), buf.size());
        input_strings.push_back(std::move(buf));
    }

    for (size_t i = 0; i < 1000; ++i) {
        std::string s(100 + (i % 200), 'X');  // Varying large strings
        s += std::to_string(i);
        auto buf = seastar::temporary_buffer<uint8_t>(s.size());
        std::memcpy(buf.get_write(), s.data(), s.size());
        input_views.emplace_back(buf.get(), buf.size());
        input_strings.push_back(std::move(buf));
    }

    // Put in one large batch to test reserve optimization
    encoder->put_batch(input_views.data(), input_views.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<seastar::temporary_buffer<uint8_t>> decoded;
    decoded.resize(input_views.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input_views.size());
    for (size_t i = 0; i < decoded.size(); ++i) {
        BOOST_CHECK_EQUAL(decoded[i].size(), input_views[i].size());
        BOOST_CHECK(std::equal(decoded[i].begin(), decoded[i].end(),
                               input_views[i].begin(), input_views[i].end()));
    }

    return seastar::async([]() {});
}

// Test plain FIXED_LEN_BYTE_ARRAY encoder with large batch
// Validates the optimized put_batch() with exact size pre-reservation
SEASTAR_TEST_CASE(plain_fixed_len_byte_array_large_batch) {
    using namespace parquet4seastar;

    constexpr size_t FIXED_LEN = 16;
    auto encoder = make_value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>(FIXED_LEN);

    // Generate many fixed-length values
    std::vector<seastar::temporary_buffer<uint8_t>> input_values;
    std::vector<bytes_view> input_views;
    input_values.reserve(3000);
    input_views.reserve(3000);

    std::mt19937 rng(42);
    std::uniform_int_distribution<uint8_t> byte_dist(0, 255);

    for (size_t i = 0; i < 3000; ++i) {
        auto buf = seastar::temporary_buffer<uint8_t>(FIXED_LEN);
        for (size_t j = 0; j < FIXED_LEN; ++j) {
            buf.get_write()[j] = byte_dist(rng);
        }
        input_views.emplace_back(buf.get(), buf.size());
        input_values.push_back(std::move(buf));
    }

    // Put in one large batch to test fixed-size reserve optimization
    encoder->put_batch(input_views.data(), input_views.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);
    BOOST_CHECK_EQUAL(n_written, FIXED_LEN * input_views.size());

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<seastar::temporary_buffer<uint8_t>> decoded;
    decoded.resize(input_views.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input_views.size());
    for (size_t i = 0; i < decoded.size(); ++i) {
        BOOST_CHECK_EQUAL(decoded[i].size(), FIXED_LEN);
        BOOST_CHECK(std::equal(decoded[i].begin(), decoded[i].end(),
                               input_views[i].begin(), input_views[i].end()));
    }

    return seastar::async([]() {});
}

// Test delta binary packed with edge case: single value
// Validates first element special handling in optimized loop
SEASTAR_TEST_CASE(delta_binary_packed_single_value) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input = {42};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
    std::vector<int32_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

// Test delta binary packed with negative deltas
// Validates min_delta calculation in combined loop
SEASTAR_TEST_CASE(delta_binary_packed_negative_deltas) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::DELTA_BINARY_PACKED);
    auto decoder = value_decoder<format::Type::INT64>({});

    // Descending sequence with varying negative deltas
    std::vector<int64_t> input;
    int64_t val = 1000000;
    for (int i = 0; i < 200; ++i) {
        input.push_back(val);
        val -= (i * i + 1);  // Increasingly negative deltas
    }

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
    std::vector<int64_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

// Test DELTA_BYTE_ARRAY decoder with large batch
// Validates the optimized exponential growth reserve() strategy
SEASTAR_TEST_CASE(delta_byte_array_large_batch) {
    using namespace parquet4seastar;

    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::DELTA_BYTE_ARRAY);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    // Generate large dataset (> 4000 values to exceed initial reserve)
    std::vector<seastar::temporary_buffer<uint8_t>> input_strings;
    std::vector<bytes_view> input_views;
    input_strings.reserve(6000);
    input_views.reserve(6000);

    // Create strings with common prefixes to benefit from delta encoding
    std::string base = "https://example.com/api/v1/users/";
    for (size_t i = 0; i < 6000; ++i) {
        std::string s = base + std::to_string(i) + "/profile?detailed=true";
        auto buf = seastar::temporary_buffer<uint8_t>(s.size());
        std::memcpy(buf.get_write(), s.data(), s.size());
        input_views.emplace_back(buf.get(), buf.size());
        input_strings.push_back(std::move(buf));
    }

    encoder->put_batch(input_views.data(), input_views.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_BYTE_ARRAY);

    std::vector<seastar::temporary_buffer<uint8_t>> decoded;
    decoded.resize(input_views.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input_views.size());
    for (size_t i = 0; i < decoded.size(); ++i) {
        BOOST_CHECK_EQUAL(decoded[i].size(), input_views[i].size());
        BOOST_CHECK(std::equal(decoded[i].begin(), decoded[i].end(),
                               input_views[i].begin(), input_views[i].end()));
    }

    return seastar::async([]() {});
}

// Test DELTA_BYTE_ARRAY with growing string pattern
// Validates the optimized last_string reserve() to avoid reallocations
SEASTAR_TEST_CASE(delta_byte_array_growing_strings) {
    using namespace parquet4seastar;

    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::DELTA_BYTE_ARRAY);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    // Generate strings that grow progressively to stress-test reallocation
    std::vector<seastar::temporary_buffer<uint8_t>> input_strings;
    std::vector<bytes_view> input_views;
    input_strings.reserve(500);
    input_views.reserve(500);

    // Pattern: strings that share growing prefixes
    for (size_t i = 0; i < 500; ++i) {
        // Each string shares prefix with previous, but grows
        std::string s(i, 'A');  // Growing prefix
        s += "_suffix_" + std::to_string(i);
        auto buf = seastar::temporary_buffer<uint8_t>(s.size());
        std::memcpy(buf.get_write(), s.data(), s.size());
        input_views.emplace_back(buf.get(), buf.size());
        input_strings.push_back(std::move(buf));
    }

    encoder->put_batch(input_views.data(), input_views.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::DELTA_BYTE_ARRAY);

    std::vector<seastar::temporary_buffer<uint8_t>> decoded;
    decoded.resize(input_views.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input_views.size());
    for (size_t i = 0; i < decoded.size(); ++i) {
        BOOST_CHECK_EQUAL(decoded[i].size(), input_views[i].size());
        BOOST_CHECK(std::equal(decoded[i].begin(), decoded[i].end(),
                               input_views[i].begin(), input_views[i].end()));
    }

    return seastar::async([]() {});
}

// Test dictionary decoder with large batch
// Validates the increased buffer size (256 -> 1024) optimization
SEASTAR_TEST_CASE(dictionary_decoder_large_batch) {
    using namespace parquet4seastar;

    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::RLE_DICTIONARY);

    // Generate data with moderate cardinality (500 unique values)
    // but 10000 total values to stress-test batch processing
    std::vector<int32_t> input;
    input.reserve(10000);

    for (int i = 0; i < 10000; ++i) {
        input.push_back(i % 500);  // 500 unique values, repeated
    }

    encoder->put_batch(input.data(), input.size());

    bytes dict_page(encoder->view_dict()->size(), 0);
    std::copy(encoder->view_dict()->begin(), encoder->view_dict()->end(), dict_page.begin());

    bytes data_page(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(data_page.data());
    data_page.resize(n_written);

    // Decode dictionary page
    auto dict_decoder = value_decoder<format::Type::INT32>({});
    dict_decoder.reset(dict_page, format::Encoding::PLAIN);
    std::vector<int32_t> dict(500);
    dict_decoder.read_batch(500, dict.data());

    // Decode data page
    auto data_decoder = value_decoder<format::Type::INT32>({});
    data_decoder.reset_dict(dict.data(), 500);
    data_decoder.reset(data_page, format::Encoding::RLE_DICTIONARY);

    std::vector<int32_t> decoded(input.size());
    size_t n_read = data_decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}
