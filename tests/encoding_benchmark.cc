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
 * Copyright (C) 2024 ScyllaDB
 */

#include <parquet4seastar/encoding.hh>
#include <parquet4seastar/compression.hh>
#include <parquet4seastar/column_chunk_reader.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <chrono>
#include <random>
#include <iomanip>

using namespace parquet4seastar;

// Benchmark timer
class benchmark_timer {
    std::chrono::high_resolution_clock::time_point _start;
    std::chrono::high_resolution_clock::time_point _end;
public:
    void start() {
        _start = std::chrono::high_resolution_clock::now();
    }

    void stop() {
        _end = std::chrono::high_resolution_clock::now();
    }

    double elapsed_ms() const {
        return std::chrono::duration<double, std::milli>(_end - _start).count();
    }

    double throughput_mb_per_sec(size_t bytes) const {
        double seconds = std::chrono::duration<double>(_end - _start).count();
        return (bytes / (1024.0 * 1024.0)) / seconds;
    }
};

// Benchmark result
struct benchmark_result {
    std::string name;
    size_t iterations;
    size_t data_size_bytes;
    double encode_time_ms;
    double decode_time_ms;
    double encode_throughput_mb_s;
    double decode_throughput_mb_s;

    void print() const {
        std::cout << std::left << std::setw(40) << name
                  << " | Encode: " << std::setw(8) << std::fixed << std::setprecision(2) << encode_time_ms << " ms"
                  << " (" << std::setw(8) << encode_throughput_mb_s << " MB/s)"
                  << " | Decode: " << std::setw(8) << decode_time_ms << " ms"
                  << " (" << std::setw(8) << decode_throughput_mb_s << " MB/s)"
                  << std::endl;
    }
};

// Benchmark plain encoding for INT32
benchmark_result benchmark_plain_int32(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "Plain INT32";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(int32_t);

    // Generate test data
    std::vector<int32_t> input(count);
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> dist;
    for (auto& v : input) {
        v = dist(rng);
    }

    // Benchmark encoding
    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::PLAIN);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    // Benchmark decoding
    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::INT32>({});
        decoder.reset(encoded, format::Encoding::PLAIN);
        std::vector<int32_t> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark plain encoding for INT64
benchmark_result benchmark_plain_int64(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "Plain INT64";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(int64_t);

    std::vector<int64_t> input(count);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist;
    for (auto& v : input) {
        v = dist(rng);
    }

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::PLAIN);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::INT64>({});
        decoder.reset(encoded, format::Encoding::PLAIN);
        std::vector<int64_t> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark plain encoding for FLOAT
benchmark_result benchmark_plain_float(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "Plain FLOAT";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(float);

    std::vector<float> input(count);
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist;
    for (auto& v : input) {
        v = dist(rng);
    }

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::FLOAT>(format::Encoding::PLAIN);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::FLOAT>({});
        decoder.reset(encoded, format::Encoding::PLAIN);
        std::vector<float> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark plain encoding for DOUBLE
benchmark_result benchmark_plain_double(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "Plain DOUBLE";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(double);

    std::vector<double> input(count);
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist;
    for (auto& v : input) {
        v = dist(rng);
    }

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::DOUBLE>(format::Encoding::PLAIN);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::DOUBLE>({});
        decoder.reset(encoded, format::Encoding::PLAIN);
        std::vector<double> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark DELTA_BINARY_PACKED encoding
benchmark_result benchmark_delta_binary_packed(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "DELTA_BINARY_PACKED INT64";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(int64_t);

    // Generate sequential-ish data (good for delta encoding)
    std::vector<int64_t> input(count);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist(-1000, 1000);
    int64_t value = 0;
    for (auto& v : input) {
        value += dist(rng);
        v = value;
    }

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::INT64>(format::Encoding::DELTA_BINARY_PACKED);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::INT64>({});
        decoder.reset(encoded, format::Encoding::DELTA_BINARY_PACKED);
        std::vector<int64_t> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark dictionary encoding
benchmark_result benchmark_dictionary_encoding(size_t count, size_t unique_values, size_t iterations) {
    benchmark_result result;
    result.name = "Dictionary INT32 (" + std::to_string(unique_values) + " unique)";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(int32_t);

    // Generate data with limited unique values
    std::vector<int32_t> input(count);
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> dist(0, unique_values - 1);
    for (auto& v : input) {
        v = dist(rng);
    }

    benchmark_timer timer;
    bytes encoded_indices;
    bytes encoded_dict;
    std::vector<int32_t> dict_values;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::INT32>(format::Encoding::RLE_DICTIONARY);
        encoder->put_batch(input.data(), input.size());

        bytes idx_buf(encoder->max_encoded_size(), 0);
        auto [idx_n, idx_enc] = encoder->flush(idx_buf.data());

        if (i == 0) {
            encoded_indices = bytes(idx_buf.data(), idx_buf.data() + idx_n);

            // Get dictionary
            auto dict_view = *encoder->view_dict();
            encoded_dict = bytes(dict_view.begin(), dict_view.end());

            // Decode dictionary values
            auto dict_decoder = value_decoder<format::Type::INT32>({});
            dict_decoder.reset(encoded_dict, format::Encoding::PLAIN);
            dict_values.resize(unique_values);
            dict_decoder.read_batch(unique_values, dict_values.data());
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::INT32>({});
        decoder.reset_dict(dict_values.data(), dict_values.size());
        decoder.reset(encoded_indices, format::Encoding::RLE_DICTIONARY);
        std::vector<int32_t> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark PLAIN encoding for strings (BYTE_ARRAY)
benchmark_result benchmark_plain_byte_array(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "Plain BYTE_ARRAY (strings)";

    // Generate strings with varying lengths
    std::vector<bytes_view> input;
    std::vector<bytes> string_storage;
    std::string base = "https://example.com/api/v1/users/";

    size_t total_bytes = 0;
    for (size_t i = 0; i < count; ++i) {
        std::string s = base + std::to_string(i) + "/profile?detailed=true";
        string_storage.emplace_back(reinterpret_cast<const uint8_t*>(s.data()),
                                    reinterpret_cast<const uint8_t*>(s.data() + s.size()));
        input.push_back(string_storage.back());
        total_bytes += s.size();
    }

    result.iterations = iterations;
    result.data_size_bytes = total_bytes;

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::BYTE_ARRAY>(format::Encoding::PLAIN);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::BYTE_ARRAY>({});
        decoder.reset(encoded, format::Encoding::PLAIN);
        std::vector<seastar::temporary_buffer<uint8_t>> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark BYTE_STREAM_SPLIT encoding (for FLOAT)
benchmark_result benchmark_byte_stream_split_float(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "BYTE_STREAM_SPLIT FLOAT";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(float);

    std::vector<float> input(count);
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist;
    for (auto& v : input) {
        v = dist(rng);
    }

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::FLOAT>(format::Encoding::BYTE_STREAM_SPLIT);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::FLOAT>({});
        decoder.reset(encoded, format::Encoding::BYTE_STREAM_SPLIT);
        std::vector<float> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark BYTE_STREAM_SPLIT encoding (for DOUBLE)
benchmark_result benchmark_byte_stream_split_double(size_t count, size_t iterations) {
    benchmark_result result;
    result.name = "BYTE_STREAM_SPLIT DOUBLE";
    result.iterations = iterations;
    result.data_size_bytes = count * sizeof(double);

    std::vector<double> input(count);
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist;
    for (auto& v : input) {
        v = dist(rng);
    }

    benchmark_timer timer;
    bytes encoded;

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto encoder = make_value_encoder<format::Type::DOUBLE>(format::Encoding::BYTE_STREAM_SPLIT);
        encoder->put_batch(input.data(), input.size());
        bytes buf(encoder->max_encoded_size(), 0);
        auto [n, enc] = encoder->flush(buf.data());
        if (i == 0) {
            encoded = bytes(buf.data(), buf.data() + n);
        }
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        auto decoder = value_decoder<format::Type::DOUBLE>({});
        decoder.reset(encoded, format::Encoding::BYTE_STREAM_SPLIT);
        std::vector<double> output(count);
        decoder.read_batch(count, output.data());
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

// Benchmark compression
benchmark_result benchmark_compression(format::CompressionCodec::type codec, size_t count, size_t iterations) {
    benchmark_result result;

    // Set codec name
    switch (codec) {
        case format::CompressionCodec::ZSTD:
            result.name = "ZSTD Compression";
            break;
        case format::CompressionCodec::SNAPPY:
            result.name = "SNAPPY Compression";
            break;
        case format::CompressionCodec::GZIP:
            result.name = "GZIP Compression";
            break;
        default:
            result.name = "UNCOMPRESSED";
            break;
    }

    // Generate test data
    bytes raw_data;
    for (size_t i = 0; i < count; ++i) {
        raw_data.push_back(static_cast<byte>(i % 256));
    }

    result.iterations = iterations;
    result.data_size_bytes = raw_data.size();

    auto compressor_inst = compressor::make(codec);
    benchmark_timer timer;
    bytes compressed;

    // Benchmark compression
    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        compressed = compressor_inst->compress(raw_data);
    }
    timer.stop();
    result.encode_time_ms = timer.elapsed_ms() / iterations;
    result.encode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    // Benchmark decompression
    bytes decompression_buffer(raw_data.size() + 1000, 0);
    timer.start();
    for (size_t i = 0; i < iterations; ++i) {
        bytes decompressed = compressor_inst->decompress(compressed, bytes(decompression_buffer));
    }
    timer.stop();
    result.decode_time_ms = timer.elapsed_ms() / iterations;
    result.decode_throughput_mb_s = timer.throughput_mb_per_sec(result.data_size_bytes * iterations);

    return result;
}

int main(int argc, char** argv) {
    seastar::app_template app;

    return app.run(argc, argv, [] {
        return seastar::async([] {
            std::cout << "\n=== Parquet4Seastar Encoding Benchmark ===" << std::endl;
            std::cout << "Compiled with O3 optimizations" << std::endl;
            std::cout << std::endl;

            const size_t small_count = 10000;
            const size_t large_count = 100000;
            const size_t iterations = 100;

            std::vector<benchmark_result> results;

            std::cout << "Running benchmarks..." << std::endl;
            std::cout << std::string(120, '=') << std::endl;

            // Plain encoding benchmarks
            std::cout << "\n[Plain Encoding - " << small_count << " values, " << iterations << " iterations]" << std::endl;
            results.push_back(benchmark_plain_int32(small_count, iterations));
            results.back().print();

            results.push_back(benchmark_plain_int64(small_count, iterations));
            results.back().print();

            results.push_back(benchmark_plain_float(small_count, iterations));
            results.back().print();

            results.push_back(benchmark_plain_double(small_count, iterations));
            results.back().print();

            // Delta binary packed
            std::cout << "\n[DELTA_BINARY_PACKED - " << small_count << " values, " << iterations << " iterations]" << std::endl;
            results.push_back(benchmark_delta_binary_packed(small_count, iterations));
            results.back().print();

            // Dictionary encoding
            std::cout << "\n[Dictionary Encoding - " << small_count << " values, " << iterations << " iterations]" << std::endl;
            results.push_back(benchmark_dictionary_encoding(small_count, 100, iterations));
            results.back().print();

            results.push_back(benchmark_dictionary_encoding(small_count, 1000, iterations));
            results.back().print();

            // String encoding (BYTE_ARRAY)
            std::cout << "\n[String Encoding - " << 5000 << " strings, " << iterations << " iterations]" << std::endl;
            results.push_back(benchmark_plain_byte_array(5000, iterations));
            results.back().print();

            // Compression benchmarks
            std::cout << "\n[Compression - " << small_count << " bytes, " << iterations << " iterations]" << std::endl;
            results.push_back(benchmark_compression(format::CompressionCodec::ZSTD, small_count, iterations));
            results.back().print();

            results.push_back(benchmark_compression(format::CompressionCodec::SNAPPY, small_count, iterations));
            results.back().print();

            results.push_back(benchmark_compression(format::CompressionCodec::GZIP, small_count, iterations));
            results.back().print();

            // Large dataset benchmarks
            std::cout << "\n[Large Dataset - " << large_count << " values, " << iterations/10 << " iterations]" << std::endl;
            results.push_back(benchmark_plain_int64(large_count, iterations/10));
            results.back().print();

            results.push_back(benchmark_delta_binary_packed(large_count, iterations/10));
            results.back().print();

            results.push_back(benchmark_dictionary_encoding(large_count, 500, iterations/10));
            results.back().print();

            std::cout << "\n" << std::string(120, '=') << std::endl;
            std::cout << "Benchmark completed successfully!" << std::endl;
            std::cout << "\nNote: To compare with pre-optimization performance, build with commit before optimizations" << std::endl;
            std::cout << "      and run the same benchmark, then compare throughput numbers." << std::endl;
        });
    });
}
