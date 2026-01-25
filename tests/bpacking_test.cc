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
#include <cstdint>
#include <cstring>
#include <parquet4seastar/bpacking.hh>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <vector>

// Helper function to pack values into bytes with a given bit width
static void pack_values(const std::vector<uint32_t>& values, std::vector<uint32_t>& packed, int bit_width) {
    if (bit_width == 0) {
        return;
    }
    size_t total_bits = values.size() * bit_width;
    size_t total_words = (total_bits + 31) / 32;
    packed.resize(total_words, 0);

    size_t bit_pos = 0;
    for (uint32_t v : values) {
        size_t word_idx = bit_pos / 32;
        size_t bit_offset = bit_pos % 32;

        packed[word_idx] |= (v << bit_offset);
        if (bit_offset + bit_width > 32 && word_idx + 1 < packed.size()) {
            packed[word_idx + 1] |= (v >> (32 - bit_offset));
        }
        bit_pos += bit_width;
    }
}

SEASTAR_TEST_CASE(unpack_bit_width_0) {
    std::vector<uint32_t> packed = {0};  // Doesn't matter for bit_width 0
    std::array<uint32_t, 32> out;
    out.fill(0xFF);  // Fill with non-zero to verify zeros are written

    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 0);
    BOOST_CHECK_EQUAL(n, 32);

    for (size_t i = 0; i < 32; ++i) {
        BOOST_CHECK_EQUAL(out[i], 0u);
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_bit_width_1) {
    // Pack 32 values with bit width 1: alternating 0 and 1
    std::vector<uint32_t> expected(32);
    for (size_t i = 0; i < 32; ++i) {
        expected[i] = i % 2;
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 1);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 1);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_bit_width_8) {
    // Pack 32 values with bit width 8
    std::vector<uint32_t> expected(32);
    for (size_t i = 0; i < 32; ++i) {
        expected[i] = static_cast<uint32_t>(i * 8);  // 0, 8, 16, ..., 248
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 8);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 8);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_bit_width_16) {
    // Pack 32 values with bit width 16
    std::vector<uint32_t> expected(32);
    for (size_t i = 0; i < 32; ++i) {
        expected[i] = static_cast<uint32_t>(i * 2000);
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 16);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 16);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_bit_width_32) {
    // Pack 32 values with bit width 32 (full uint32_t)
    std::vector<uint32_t> expected(32);
    for (size_t i = 0; i < 32; ++i) {
        expected[i] = static_cast<uint32_t>(i * 100000000 + 12345678);
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 32);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 32);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_all_bit_widths) {
    // Test all bit widths from 0 to 32
    for (int bit_width = 0; bit_width <= 32; ++bit_width) {
        std::vector<uint32_t> expected(32);
        uint32_t max_val = (bit_width == 32) ? 0xFFFFFFFF : ((1u << bit_width) - 1);
        for (size_t i = 0; i < 32; ++i) {
            expected[i] = static_cast<uint32_t>(i) % (max_val + 1);
        }

        std::vector<uint32_t> packed;
        pack_values(expected, packed, bit_width);

        std::array<uint32_t, 32> out;
        int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, bit_width);
        BOOST_CHECK_EQUAL(n, 32);

        for (size_t i = 0; i < 32; ++i) {
            BOOST_CHECK_MESSAGE(out[i] == expected[i],
                                "Mismatch at bit_width=" << bit_width << ", i=" << i << ": expected " << expected[i]
                                                         << ", got " << out[i]);
        }
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_multiple_batches) {
    // Test unpacking multiple batches of 32 values
    constexpr int bit_width = 5;
    constexpr size_t num_batches = 4;
    constexpr size_t total_values = num_batches * 32;

    std::vector<uint32_t> expected(total_values);
    for (size_t i = 0; i < total_values; ++i) {
        expected[i] = static_cast<uint32_t>(i % 32);  // Values 0-31
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, bit_width);

    std::vector<uint32_t> out(total_values);
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), total_values, bit_width);
    BOOST_CHECK_EQUAL(static_cast<size_t>(n), total_values);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_batch_size_truncation) {
    // Test that batch_size is truncated to multiple of 32
    constexpr int bit_width = 4;
    std::vector<uint32_t> expected(32);
    for (size_t i = 0; i < 32; ++i) {
        expected[i] = static_cast<uint32_t>(i % 16);
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, bit_width);

    std::vector<uint32_t> out(64);
    // Request 50 values, should only get 32 (truncated to multiple of 32)
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 50, bit_width);
    BOOST_CHECK_EQUAL(n, 32);

    // Only first 32 values should match
    for (size_t i = 0; i < 32; ++i) {
        BOOST_CHECK_EQUAL(out[i], expected[i]);
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_known_patterns_bit_width_3) {
    // Test with a known hand-crafted pattern for bit width 3
    // Values: 0, 1, 2, 3, 4, 5, 6, 7 (repeated 4 times = 32 values)
    std::vector<uint32_t> expected(32);
    for (size_t i = 0; i < 32; ++i) {
        expected[i] = static_cast<uint32_t>(i % 8);
    }

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 3);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 3);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_max_values_all_widths) {
    // Test with maximum values for each bit width
    for (int bit_width = 1; bit_width <= 32; ++bit_width) {
        uint32_t max_val = (bit_width == 32) ? 0xFFFFFFFF : ((1u << bit_width) - 1);
        std::vector<uint32_t> expected(32, max_val);

        std::vector<uint32_t> packed;
        pack_values(expected, packed, bit_width);

        std::array<uint32_t, 32> out;
        int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, bit_width);
        BOOST_CHECK_EQUAL(n, 32);

        for (size_t i = 0; i < 32; ++i) {
            BOOST_CHECK_MESSAGE(out[i] == max_val,
                                "Mismatch at bit_width=" << bit_width << ", i=" << i << ": expected " << max_val
                                                         << ", got " << out[i]);
        }
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_random_values) {
    // Test with random values for each bit width
    std::mt19937 rng(42);  // Fixed seed for reproducibility

    for (int bit_width = 1; bit_width <= 32; ++bit_width) {
        uint32_t max_val = (bit_width == 32) ? 0xFFFFFFFF : ((1u << bit_width) - 1);
        std::uniform_int_distribution<uint32_t> dist(0, max_val);

        std::vector<uint32_t> expected(32);
        for (size_t i = 0; i < 32; ++i) {
            expected[i] = dist(rng);
        }

        std::vector<uint32_t> packed;
        pack_values(expected, packed, bit_width);

        std::array<uint32_t, 32> out;
        int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, bit_width);
        BOOST_CHECK_EQUAL(n, 32);

        for (size_t i = 0; i < 32; ++i) {
            BOOST_CHECK_MESSAGE(out[i] == expected[i],
                                "Mismatch at bit_width=" << bit_width << ", i=" << i << ": expected " << expected[i]
                                                         << ", got " << out[i]);
        }
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_empty_batch) {
    // Test with batch size 0
    std::array<uint32_t, 32> out;
    out.fill(0xDEADBEEF);  // Fill with sentinel value

    int n = parquet4seastar::internal::unpack32(nullptr, out.data(), 0, 5);
    BOOST_CHECK_EQUAL(n, 0);

    // Output should be unchanged
    for (size_t i = 0; i < 32; ++i) {
        BOOST_CHECK_EQUAL(out[i], 0xDEADBEEFu);
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_small_batch_truncated_to_zero) {
    // Test with batch size < 32 (should be truncated to 0)
    std::array<uint32_t, 32> out;
    out.fill(0xDEADBEEF);

    int n = parquet4seastar::internal::unpack32(nullptr, out.data(), 31, 5);
    BOOST_CHECK_EQUAL(n, 0);

    // Output should be unchanged
    for (size_t i = 0; i < 32; ++i) {
        BOOST_CHECK_EQUAL(out[i], 0xDEADBEEFu);
    }

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_single_bit_all_ones) {
    // Test bit width 1 with all ones
    std::vector<uint32_t> expected(32, 1);

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 1);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 1);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}

SEASTAR_TEST_CASE(unpack_single_bit_all_zeros) {
    // Test bit width 1 with all zeros
    std::vector<uint32_t> expected(32, 0);

    std::vector<uint32_t> packed;
    pack_values(expected, packed, 1);

    std::array<uint32_t, 32> out;
    int n = parquet4seastar::internal::unpack32(packed.data(), out.data(), 32, 1);
    BOOST_CHECK_EQUAL(n, 32);

    BOOST_CHECK_EQUAL_COLLECTIONS(out.begin(), out.end(), expected.begin(), expected.end());

    return seastar::async([]() {});
}
