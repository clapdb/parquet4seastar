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

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <parquet4seastar/encoding.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <vector>

constexpr parquet4seastar::bytes_view operator""_bv(const char* str, size_t len) noexcept {
    return {static_cast<const uint8_t*>(static_cast<const void*>(str)), len};
}

SEASTAR_TEST_CASE(plain_int32_roundtrip) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input = {0, 1, -1, 100, -100, 12345, -67890};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int32_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_int64_roundtrip) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT64>({});

    std::vector<int64_t> input = {0, 1, -1, 1000000000000LL, -9876543210LL};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int64_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_float_roundtrip) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::FLOAT>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::FLOAT>({});

    std::vector<float> input = {0.0f, 1.0f, -1.0f, 3.14159f, -2.71828f, 1e10f, 1e-10f};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<float> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        BOOST_CHECK_CLOSE(input[i], decoded[i], 1e-5);
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_double_roundtrip) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::DOUBLE>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::DOUBLE>({});

    std::vector<double> input = {0.0, 1.0, -1.0, 3.141592653589793, -2.718281828459045, 1e100, 1e-100};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<double> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        BOOST_CHECK_CLOSE(input[i], decoded[i], 1e-10);
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_boolean_roundtrip) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::BOOLEAN>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::BOOLEAN>({});

    std::vector<uint8_t> input = {1, 0, 1, 1, 0, 0, 1, 0, 1};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<uint8_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_byte_array_roundtrip) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    bytes_view input[] = {"hello"_bv, "world"_bv, ""_bv, "test"_bv};
    encoder->put_batch(input, std::size(input));

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<seastar::temporary_buffer<uint8_t>> decoded(std::size(input));
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    BOOST_CHECK_EQUAL(n_read, std::size(input));

    for (size_t i = 0; i < std::size(input); ++i) {
        bytes_view expected = input[i];
        bytes_view actual(decoded[i].get(), decoded[i].size());
        BOOST_CHECK_EQUAL_COLLECTIONS(expected.begin(), expected.end(), actual.begin(), actual.end());
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_fixed_len_byte_array_roundtrip) {
    using namespace parquet4seastar;
    constexpr uint32_t fixed_len = 4;
    auto encoder = make_value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>(fixed_len);

    bytes_view input[] = {"aaaa"_bv, "bbbb"_bv, "cccc"_bv, "dddd"_bv};
    encoder->put_batch(input, std::size(input));

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(encoding, format::Encoding::PLAIN);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<seastar::temporary_buffer<uint8_t>> decoded(std::size(input));
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    BOOST_CHECK_EQUAL(n_read, std::size(input));

    for (size_t i = 0; i < std::size(input); ++i) {
        bytes_view expected = input[i];
        bytes_view actual(decoded[i].get(), decoded[i].size());
        BOOST_CHECK_EQUAL_COLLECTIONS(expected.begin(), expected.end(), actual.begin(), actual.end());
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_edge_values_int32) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input = {std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max(), 0, -1, 1};

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int32_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_edge_values_int64) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT64>({});

    std::vector<int64_t> input = {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max(), 0, -1, 1};

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int64_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_edge_values_float) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::FLOAT>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::FLOAT>({});

    std::vector<float> input = {std::numeric_limits<float>::min(),
                                std::numeric_limits<float>::max(),
                                std::numeric_limits<float>::lowest(),
                                std::numeric_limits<float>::infinity(),
                                -std::numeric_limits<float>::infinity(),
                                std::numeric_limits<float>::denorm_min(),
                                0.0f,
                                -0.0f};

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<float> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        if (std::isinf(input[i])) {
            BOOST_CHECK(std::isinf(decoded[i]));
            BOOST_CHECK_EQUAL(std::signbit(input[i]), std::signbit(decoded[i]));
        } else {
            BOOST_CHECK_EQUAL(input[i], decoded[i]);
        }
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_edge_values_float_nan) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::FLOAT>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::FLOAT>({});

    std::vector<float> input = {std::numeric_limits<float>::quiet_NaN()};

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<float> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    BOOST_CHECK(std::isnan(decoded[0]));

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_edge_values_double) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::DOUBLE>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::DOUBLE>({});

    std::vector<double> input = {std::numeric_limits<double>::min(),
                                 std::numeric_limits<double>::max(),
                                 std::numeric_limits<double>::lowest(),
                                 std::numeric_limits<double>::infinity(),
                                 -std::numeric_limits<double>::infinity(),
                                 std::numeric_limits<double>::denorm_min(),
                                 0.0,
                                 -0.0};

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<double> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        if (std::isinf(input[i])) {
            BOOST_CHECK(std::isinf(decoded[i]));
            BOOST_CHECK_EQUAL(std::signbit(input[i]), std::signbit(decoded[i]));
        } else {
            BOOST_CHECK_EQUAL(input[i], decoded[i]);
        }
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_edge_values_double_nan) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::DOUBLE>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::DOUBLE>({});

    std::vector<double> input = {std::numeric_limits<double>::quiet_NaN()};

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<double> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL(decoded.size(), input.size());
    BOOST_CHECK(std::isnan(decoded[0]));

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_empty_batch_int32) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input;
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size() + 1, 0);  // +1 to avoid zero-size allocation
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(n_written, 0u);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int32_t> decoded(10);
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    BOOST_CHECK_EQUAL(n_read, 0u);

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_empty_batch_byte_array) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    encoder->put_batch(nullptr, 0);

    bytes encoded(encoder->max_encoded_size() + 1, 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);
    BOOST_CHECK_EQUAL(n_written, 0u);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<seastar::temporary_buffer<uint8_t>> decoded(10);
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    BOOST_CHECK_EQUAL(n_read, 0u);

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_partial_read_int32) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);

    // Read first 3 values
    std::vector<int32_t> decoded1(3);
    size_t n_read1 = decoder.read_batch(decoded1.size(), decoded1.data());
    BOOST_CHECK_EQUAL(n_read1, 3u);
    std::vector<int32_t> expected1 = {1, 2, 3};
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded1.begin(), decoded1.end(), expected1.begin(), expected1.end());

    // Read next 4 values
    std::vector<int32_t> decoded2(4);
    size_t n_read2 = decoder.read_batch(decoded2.size(), decoded2.data());
    BOOST_CHECK_EQUAL(n_read2, 4u);
    std::vector<int32_t> expected2 = {4, 5, 6, 7};
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded2.begin(), decoded2.end(), expected2.begin(), expected2.end());

    // Read remaining values
    std::vector<int32_t> decoded3(10);
    size_t n_read3 = decoder.read_batch(decoded3.size(), decoded3.data());
    BOOST_CHECK_EQUAL(n_read3, 3u);
    decoded3.resize(n_read3);
    std::vector<int32_t> expected3 = {8, 9, 10};
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded3.begin(), decoded3.end(), expected3.begin(), expected3.end());

    // No more values
    std::vector<int32_t> decoded4(10);
    size_t n_read4 = decoder.read_batch(decoded4.size(), decoded4.data());
    BOOST_CHECK_EQUAL(n_read4, 0u);

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_partial_read_byte_array) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});

    bytes_view input[] = {"aa"_bv, "bb"_bv, "cc"_bv, "dd"_bv, "ee"_bv};
    encoder->put_batch(input, std::size(input));

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);

    // Read first 2 values
    std::vector<seastar::temporary_buffer<uint8_t>> decoded1(2);
    size_t n_read1 = decoder.read_batch(decoded1.size(), decoded1.data());
    BOOST_CHECK_EQUAL(n_read1, 2u);
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded1[0].begin(), decoded1[0].end(), input[0].begin(), input[0].end());
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded1[1].begin(), decoded1[1].end(), input[1].begin(), input[1].end());

    // Read remaining values
    std::vector<seastar::temporary_buffer<uint8_t>> decoded2(10);
    size_t n_read2 = decoder.read_batch(decoded2.size(), decoded2.data());
    BOOST_CHECK_EQUAL(n_read2, 3u);
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded2[0].begin(), decoded2[0].end(), input[2].begin(), input[2].end());
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded2[1].begin(), decoded2[1].end(), input[3].begin(), input[3].end());
    BOOST_CHECK_EQUAL_COLLECTIONS(decoded2[2].begin(), decoded2[2].end(), input[4].begin(), input[4].end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_large_batch_int32) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input;
    for (int32_t i = 0; i < 10000; ++i) {
        input.push_back(i);
    }

    encoder->put_batch(input.data(), input.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int32_t> decoded(input.size());
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    BOOST_CHECK_EQUAL_COLLECTIONS(input.begin(), input.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(plain_multiple_put_batch_int32) {
    using namespace parquet4seastar;
    auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
    auto decoder = value_decoder<format::Type::INT32>({});

    std::vector<int32_t> input1 = {1, 2, 3};
    std::vector<int32_t> input2 = {4, 5, 6, 7};
    std::vector<int32_t> input3 = {8, 9, 10};

    encoder->put_batch(input1.data(), input1.size());
    encoder->put_batch(input2.data(), input2.size());
    encoder->put_batch(input3.data(), input3.size());

    bytes encoded(encoder->max_encoded_size(), 0);
    auto [n_written, encoding] = encoder->flush(encoded.data());
    encoded.resize(n_written);

    decoder.reset(encoded, format::Encoding::PLAIN);
    std::vector<int32_t> decoded(10);
    size_t n_read = decoder.read_batch(decoded.size(), decoded.data());
    decoded.resize(n_read);

    std::vector<int32_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    BOOST_CHECK_EQUAL_COLLECTIONS(expected.begin(), expected.end(), decoded.begin(), decoded.end());

    return seastar::async([]() {});
}
