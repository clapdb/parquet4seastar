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

#include <parquet4seastar/encoding.hh>

namespace parquet4seastar {

size_t level_decoder::reset_v1(bytes_view buffer, format::Encoding::type encoding, uint32_t num_values) {
    _num_values = num_values;
    _values_read = 0;
    if (_bit_width == 0) {
        return 0;
    }
    if (encoding == format::Encoding::RLE) {
        if (buffer.size() < 4) {
            throw parquet_exception::corrupted_file(
              seastar::format("End of page while reading levels (needed {}B, got {}B)", 4, buffer.size()));
        }
        int32_t len;
        std::memcpy(&len, buffer.data(), 4);
        if (len < 0) {
            throw parquet_exception::corrupted_file(seastar::format("Negative RLE levels length ({})", len));
        }
        if (static_cast<size_t>(len) > buffer.size()) {
            throw parquet_exception::corrupted_file(
              seastar::format("End of page while reading levels (needed {}B, got {}B)", len, buffer.size()));
        }
        _decoder = RleDecoder{buffer.data() + 4, len, static_cast<int>(_bit_width)};
        return 4 + len;
    } else if (encoding == format::Encoding::BIT_PACKED) {
        uint64_t bit_len = static_cast<uint64_t>(num_values) * _bit_width;
        uint64_t byte_len = (bit_len + 7) >> 3;
        if (byte_len > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            throw parquet_exception::corrupted_file(seastar::format("BIT_PACKED length exceeds int ({}B)", byte_len));
        }
        if (byte_len > buffer.size()) {
            throw parquet_exception::corrupted_file(
              seastar::format("End of page while reading levels (needed {}B, got {}B)", byte_len, buffer.size()));
        }
        _decoder = BitReader{buffer.data(), static_cast<int>(byte_len)};
        return byte_len;
    } else {
        throw parquet_exception(seastar::format("Unknown level encoding ({})", static_cast<int32_t>(encoding)));
    }
}

void level_decoder::reset_v2(bytes_view encoded_levels, uint32_t num_values) {
    _num_values = num_values;
    _values_read = 0;
    if (encoded_levels.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw parquet_exception::corrupted_file(
          seastar::format("Levels length exceeds int ({}B)", encoded_levels.size()));
    }
    _decoder = RleDecoder{encoded_levels.data(), static_cast<int>(encoded_levels.size()), static_cast<int>(_bit_width)};
}

template <format::Type::type ParquetType>
class plain_decoder_trivial final : public decoder<ParquetType>
{
    bytes_view _buffer;

   public:
    using typename decoder<ParquetType>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class plain_decoder_boolean final : public decoder<format::Type::BOOLEAN>
{
    BitReader _decoder;

   public:
    using typename decoder<format::Type::BOOLEAN>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class plain_decoder_byte_array final : public decoder<format::Type::BYTE_ARRAY>
{
    seastar::temporary_buffer<uint8_t> _buffer;

   public:
    using typename decoder<format::Type::BYTE_ARRAY>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class plain_decoder_fixed_len_byte_array final : public decoder<format::Type::FIXED_LEN_BYTE_ARRAY>
{
    size_t _fixed_len;
    seastar::temporary_buffer<uint8_t> _buffer;

   public:
    using typename decoder<format::Type::FIXED_LEN_BYTE_ARRAY>::output_type;
    explicit plain_decoder_fixed_len_byte_array(size_t fixed_len = 0) : _fixed_len(fixed_len) {}
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

template <format::Type::type ParquetType>
class dict_decoder final : public decoder<ParquetType>
{
   public:
    using typename decoder<ParquetType>::output_type;

   private:
    output_type* _dict;
    size_t _dict_size;
    RleDecoder _rle_decoder;

   public:
    explicit dict_decoder(output_type dict[], size_t dict_size) : _dict(dict), _dict_size(dict_size){};
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

class rle_decoder_boolean final : public decoder<format::Type::BOOLEAN>
{
    RleDecoder _rle_decoder;

   public:
    using typename decoder<format::Type::BOOLEAN>::output_type;
    void reset(bytes_view data) override;
    size_t read_batch(size_t n, output_type out[]) override;
};

template <format::Type::type ParquetType>
class delta_binary_packed_decoder final : public decoder<ParquetType>
{
    BitReader _decoder;
    uint64_t _values_per_block;
    uint64_t _num_mini_blocks;
    uint64_t _values_remaining;
    uint64_t _last_value;

    uint64_t _min_delta;
    buffer _delta_bit_widths;

    uint8_t _delta_bit_width;
    uint64_t _mini_block_idx;
    uint64_t _values_current_mini_block;
    uint64_t _values_per_mini_block;

   private:
    void init_block() {
        int64_t min_delta;
        if (!_decoder.GetZigZagVlqInt(&min_delta)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED block header");
        }
        _min_delta = min_delta;

        for (uint32_t i = 0; i < _num_mini_blocks; ++i) {
            if (!_decoder.GetAligned<uint8_t>(1, _delta_bit_widths.data() + i)) {
                throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED block header");
            }
        }
        _mini_block_idx = 0;
    }

   public:
    using typename decoder<ParquetType>::output_type;

    size_t bytes_left() { return _decoder.bytes_left(); }

    void reset(bytes_view data) override {
        _decoder.Reset(data.data(), data.size());

        if (!_decoder.GetVlqInt(&_values_per_block)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        if (!_decoder.GetVlqInt(&_num_mini_blocks)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        if (_num_mini_blocks == 0) {
            throw parquet_exception("In DELTA_BINARY_PACKED number miniblocks per block is 0");
        }
        if (!_decoder.GetVlqInt(&_values_remaining)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        int64_t first_value;
        if (!_decoder.GetZigZagVlqInt(&first_value)) {
            throw parquet_exception("Unexpected end of DELTA_BINARY_PACKED header");
        }
        _last_value = first_value;
        if (_delta_bit_widths.size() < _num_mini_blocks) {
            _delta_bit_widths = buffer(_num_mini_blocks);
        }

        _values_per_mini_block = _values_per_block / _num_mini_blocks;
        _values_current_mini_block = 0;
        _mini_block_idx = _num_mini_blocks;
    }

    size_t read_batch(size_t n, output_type out[]) override {
        if (_values_remaining == 0) {
            return 0;
        }
        size_t i = 0;
        while (i < n) {
            out[i] = _last_value;
            ++i;
            --_values_remaining;
            if (_values_remaining == 0) {
                eat_final_padding();
                break;
            }
            if (__builtin_expect(_values_current_mini_block == 0, 0)) {
                if (_mini_block_idx == _num_mini_blocks) {
                    init_block();
                }
                _delta_bit_width = _delta_bit_widths.data()[_mini_block_idx];
                _values_current_mini_block = _values_per_mini_block;
                ++_mini_block_idx;
            }
            // TODO: an optimized implementation would decode the entire
            // miniblock at once.
            uint64_t delta;
            if (!_decoder.GetValue(_delta_bit_width, &delta)) {
                throw parquet_exception("Unexpected end of data in DELTA_BINARY_PACKED");
            }
            delta += _min_delta;
            _last_value += delta;
            --_values_current_mini_block;
        }
        return i;
    }

    void eat_final_padding() {
        while (_values_current_mini_block > 0) {
            uint64_t unused_delta;
            if (!_decoder.GetValue(_delta_bit_width, &unused_delta)) {
                throw parquet_exception("Unexpected end of data in DELTA_BINARY_PACKED");
            }
            --_values_current_mini_block;
        }
    }
};

class delta_length_byte_array_decoder final : public decoder<format::Type::BYTE_ARRAY>
{
    seastar::temporary_buffer<byte> _values;
    std::vector<int32_t> _lengths;
    size_t _current_idx;
    static constexpr size_t BATCH_SIZE = 1000;

   public:
    using typename decoder<format::Type::BYTE_ARRAY>::output_type;
    size_t read_batch(size_t n, output_type out[]) override {
        n = std::min(n, _lengths.size() - _current_idx);
        for (size_t i = 0; i < n; ++i) {
            uint32_t len = _lengths[_current_idx];
            if (len > _values.size()) {
                throw parquet_exception("Unexpected end of values in DELTA_LENGTH_BYTE_ARRAY");
            }
            out[i] = _values.share(0, len);
            _values.trim_front(len);
            ++_current_idx;
        }
        return n;
    }
    void reset(bytes_view data) override {
        delta_binary_packed_decoder<format::Type::INT32> _len_decoder;
        _len_decoder.reset(data);

        size_t lengths_read = 0;
        while (true) {
            _lengths.resize(lengths_read + BATCH_SIZE);
            int32_t* output = _lengths.data() + _lengths.size() - BATCH_SIZE;
            size_t n_read = _len_decoder.read_batch(BATCH_SIZE, output);
            if (n_read == 0) {
                break;
            }
            lengths_read += n_read;
        }
        _lengths.resize(lengths_read);

        size_t len_bytes = data.size() - _len_decoder.bytes_left();
        data.remove_prefix(len_bytes);
        _values = seastar::temporary_buffer<byte>(data.data(), data.size());
        _current_idx = 0;
    }
};

class delta_byte_array_decoder final : public decoder<format::Type::BYTE_ARRAY>
{
    using tb = seastar::temporary_buffer<byte>;
    std::vector<tb> _suffixes;
    std::vector<int32_t> _lengths;
    bytes _last_string;
    size_t _current_idx;
    static constexpr size_t BATCH_SIZE = 1000;

   public:
    using typename decoder<format::Type::BYTE_ARRAY>::output_type;
    size_t read_batch(size_t n, output_type out[]) override {
        n = std::min(n, _suffixes.size() - _current_idx);
        for (size_t i = 0; i < n; ++i) {
            uint32_t prefix_len = _lengths[i];
            const tb& suffix = _suffixes[i];
            if (prefix_len > _last_string.size()) {
                throw parquet_exception("Invalid prefix length in DELTA_BYTE_ARRAY");
            }
            out[i] = tb(prefix_len + suffix.size());
            std::copy_n(_last_string.begin(), prefix_len, out[i].get_write());
            std::copy(suffix.begin(), suffix.end(), out[i].get_write() + prefix_len);
            _last_string.resize(prefix_len);
            _last_string.insert(_last_string.end(), suffix.begin(), suffix.end());
        }
        return n;
    }
    void reset(bytes_view data) override {
        delta_binary_packed_decoder<format::Type::INT32> _len_decoder;
        delta_length_byte_array_decoder _suffix_decoder;

        _len_decoder.reset(data);
        size_t lengths_read = 0;
        while (true) {
            _lengths.resize(lengths_read + BATCH_SIZE);
            int32_t* output = _lengths.data() + _lengths.size() - BATCH_SIZE;
            size_t n_read = _len_decoder.read_batch(BATCH_SIZE, output);
            if (n_read == 0) {
                break;
            }
            lengths_read += n_read;
        }
        _lengths.resize(lengths_read);

        size_t len_bytes = data.size() - _len_decoder.bytes_left();
        data.remove_prefix(len_bytes);

        _suffix_decoder.reset(data);
        size_t suffixes_read = 0;
        while (true) {
            _suffixes.resize(suffixes_read + BATCH_SIZE);
            tb* output = _suffixes.data() + _suffixes.size() - BATCH_SIZE;
            size_t n_read = _suffix_decoder.read_batch(BATCH_SIZE, output);
            if (n_read == 0) {
                break;
            }
            suffixes_read += n_read;
        }
        _suffixes.resize(suffixes_read);

        _current_idx = 0;
    }
};

template <format::Type::type ParquetType>
class byte_stream_split_decoder final : public decoder<ParquetType>
{
    bytes_view _data;
    size_t _current_idx;
    size_t _total_values;

   public:
    using typename decoder<ParquetType>::output_type;
    size_t read_batch(size_t n, output_type out[]) override {
        n = std::min(n, _total_values - _current_idx);

        byte* out_bytes = reinterpret_cast<byte*>(out);
        for (size_t i = 0; i < n; ++i) {
            for (size_t k = 0; k < sizeof(output_type); ++k) {
                size_t out_byte_idx = k + i * sizeof(output_type);
                size_t in_byte_idx = _current_idx + k * _total_values;
                out_bytes[out_byte_idx] = _data[in_byte_idx];
            }
            ++_current_idx;
        }
        return n;
    }
    void reset(bytes_view data) override {
        if (data.size() % sizeof(output_type) != 0) {
            throw parquet_exception(
              "Data size in BYTE_STREAM_SPLIT "
              "is not divisible by size of data type");
        }
        _data = data;
        _total_values = data.size() / sizeof(output_type);
        _current_idx = 0;
    }
};

template <format::Type::type ParquetType>
void plain_decoder_trivial<ParquetType>::reset(bytes_view data) {
    _buffer = data;
}

void plain_decoder_boolean::reset(bytes_view data) {
    _decoder.Reset(static_cast<const uint8_t*>(data.data()), data.size());
}

void plain_decoder_byte_array::reset(bytes_view data) {
    _buffer = seastar::temporary_buffer<uint8_t>(data.size());
    std::memcpy(_buffer.get_write(), data.data(), data.size());
}

void plain_decoder_fixed_len_byte_array::reset(bytes_view data) {
    _buffer = seastar::temporary_buffer<uint8_t>(data.size());
    std::memcpy(_buffer.get_write(), data.data(), data.size());
}

template <format::Type::type ParquetType>
size_t plain_decoder_trivial<ParquetType>::read_batch(size_t n, output_type out[]) {
    size_t n_to_read = std::min(_buffer.size() / sizeof(output_type), n);
    size_t bytes_to_read = sizeof(output_type) * n_to_read;
    if (bytes_to_read > 0) {
        std::memcpy(out, _buffer.data(), bytes_to_read);
    }
    _buffer.remove_prefix(bytes_to_read);
    return n_to_read;
}

size_t plain_decoder_boolean::read_batch(size_t n, uint8_t out[]) { return _decoder.GetBatch(1, out, n); }

size_t plain_decoder_byte_array::read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]) {
    for (size_t i = 0; i < n; ++i) {
        if (_buffer.size() == 0) {
            return i;
        }
        if (_buffer.size() < 4) {
            throw parquet_exception::corrupted_file(
              seastar::format("End of page while reading BYTE_ARRAY length (needed {}B, got {}B)", 4, _buffer.size()));
        }
        uint32_t len;
        std::memcpy(&len, _buffer.get(), 4);
        _buffer.trim_front(4);
        if (len > _buffer.size()) {
            throw parquet_exception::corrupted_file(
              seastar::format("End of page while reading BYTE_ARRAY (needed {}B, got {}B)", len, _buffer.size()));
        }
        out[i] = _buffer.share(0, len);
        _buffer.trim_front(len);
    }
    return n;
}

size_t plain_decoder_fixed_len_byte_array::read_batch(size_t n, seastar::temporary_buffer<uint8_t> out[]) {
    for (size_t i = 0; i < n; ++i) {
        if (_buffer.size() == 0) {
            return i;
        }
        if (_fixed_len > _buffer.size()) {
            throw parquet_exception::corrupted_file(seastar::format(
              "End of page while reading FIXED_LEN_BYTE_ARRAY (needed {}B, got {}B)", _fixed_len, _buffer.size()));
        }
        out[i] = _buffer.share(0, _fixed_len);
        _buffer.trim_front(_fixed_len);
    }
    return n;
}

template <format::Type::type ParquetType>
void dict_decoder<ParquetType>::reset(bytes_view data) {
    if (data.size() == 0) {
        _rle_decoder.Reset(data.data(), data.size(), 0);
    }
    int bit_width = data.data()[0];
    if (bit_width < 0 || bit_width > 32) {
        throw parquet_exception::corrupted_file(
          seastar::format("Illegal dictionary index bit width (should be 0 <= bit width <= 32, got {})", bit_width));
    }
    _rle_decoder.Reset(data.data() + 1, data.size() - 1, bit_width);
}

template <format::Type::type ParquetType>
size_t dict_decoder<ParquetType>::read_batch(size_t n, output_type out[]) {
    uint32_t buf[256];
    size_t completed = 0;
    while (completed < n) {
        size_t n_to_read = std::min(n - completed, std::size(buf));
        size_t n_read = _rle_decoder.GetBatch(std::data(buf), n_to_read);
        for (size_t i = 0; i < n_read; ++i) {
            if (buf[i] > _dict_size) {
                throw parquet_exception::corrupted_file(
                  seastar::format("Dict index exceeds dict size (dict size = {}, index = {})", _dict_size, buf[i]));
            }
            if constexpr (std::is_trivially_copyable_v<output_type>) {
                out[completed] = _dict[buf[i]];
            } else {
                // Why isn't seastar::temporary_buffer copyable though?
                out[completed] = _dict[buf[i]].share();
            }
            ++completed;
        }
        if (n_read < n_to_read) {
            break;
        }
    }
    return completed;
}

void rle_decoder_boolean::reset(bytes_view data) { _rle_decoder.Reset(data.data(), data.size(), 1); }

size_t rle_decoder_boolean::read_batch(size_t n, uint8_t out[]) { return _rle_decoder.GetBatch(out, n); }

template <format::Type::type ParquetType>
void value_decoder<ParquetType>::reset_dict(output_type dictionary[], size_t dictionary_size) {
    _dict = dictionary;
    _dict_size = dictionary_size;
    _dict_set = true;
};

template <format::Type::type ParquetType>
void value_decoder<ParquetType>::reset(bytes_view buf, format::Encoding::type encoding) {
    switch (encoding) {
        case format::Encoding::PLAIN:
            if constexpr (ParquetType == format::Type::BOOLEAN) {
                _decoder = std::make_unique<plain_decoder_boolean>();
            } else if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
                _decoder = std::make_unique<plain_decoder_byte_array>();
            } else if constexpr (ParquetType == format::Type::FIXED_LEN_BYTE_ARRAY) {
                _decoder = std::make_unique<plain_decoder_fixed_len_byte_array>(static_cast<size_t>(*_type_length));
            } else {
                _decoder = std::make_unique<plain_decoder_trivial<ParquetType>>();
            }
            break;
        case format::Encoding::RLE_DICTIONARY:
        case format::Encoding::PLAIN_DICTIONARY:
            if (!_dict_set) {
                throw parquet_exception::corrupted_file("No dictionary page found before a dictionary-encoded page");
            }
            _decoder = std::make_unique<dict_decoder<ParquetType>>(_dict, _dict_size);
            break;
        case format::Encoding::RLE:
            if constexpr (ParquetType == format::Type::BOOLEAN) {
                _decoder = std::make_unique<rle_decoder_boolean>();
            } else {
                throw parquet_exception::corrupted_file("RLE encoding is valid only for BOOLEAN values");
            }
            break;
        case format::Encoding::DELTA_BINARY_PACKED:
            if constexpr (ParquetType == format::Type::INT32 || ParquetType == format::Type::INT64) {
                _decoder = std::make_unique<delta_binary_packed_decoder<ParquetType>>();
            } else {
                throw parquet_exception::corrupted_file("DELTA_BINARY_PACKED is valid only for INT32 and INT64");
            }
            break;
        case format::Encoding::DELTA_LENGTH_BYTE_ARRAY:
            if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
                _decoder = std::make_unique<delta_length_byte_array_decoder>();
            } else {
                throw parquet_exception::corrupted_file("DELTA_LENGTH_BYTE_ARRAY is valid only for BYTE_ARRAY");
            }
            break;
        case format::Encoding::DELTA_BYTE_ARRAY:
            if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
                _decoder = std::make_unique<delta_byte_array_decoder>();
            } else {
                throw parquet_exception::corrupted_file("DELTA_BYTE_ARRAY is valid only for BYTE_ARRAY");
            }
            break;
        case format::Encoding::BYTE_STREAM_SPLIT:
            if constexpr (ParquetType == format::Type::FLOAT || ParquetType == format::Type::DOUBLE) {
                _decoder = std::make_unique<byte_stream_split_decoder<ParquetType>>();
            } else {
                throw parquet_exception::corrupted_file("BYTE_STREAM_SPLIT is valid only for FLOAT and DOUBLE");
            }
            break;
        default:
            throw parquet_exception(seastar::format("Encoding {} not implemented", static_cast<int32_t>(encoding)));
    }
    _decoder->reset(buf);
};

template <format::Type::type ParquetType>
size_t value_decoder<ParquetType>::read_batch(size_t n, output_type out[]) {
    return _decoder->read_batch(n, out);
};

/*
 * Explicit instantiation of value_decoder shouldn't be needed,
 * because column_chunk_reader<T> has a value_decoder<T> member.
 * Yet, without explicit instantiation of value_decoder<T>,
 * value_decoder<T>::read_batch is not generated. Why?
 *
 * Quote: When an explicit instantiation names a class template specialization,
 * it serves as an explicit instantiation of the same kind (declaration or definition)
 * of each of its non-inherited non-template members that has not been previously
 * explicitly specialized in the translation unit. If this explicit instantiation
 * is a definition, it is also an explicit instantiation definition only for
 * the members that have been defined at this point.
 * https://en.cppreference.com/w/cpp/language/class_template
 *
 * Has value_decoder<T> not been "defined at this point" or what?
 */
template class value_decoder<format::Type::INT32>;
template class value_decoder<format::Type::INT64>;
template class value_decoder<format::Type::INT96>;
template class value_decoder<format::Type::FLOAT>;
template class value_decoder<format::Type::DOUBLE>;
template class value_decoder<format::Type::BOOLEAN>;
template class value_decoder<format::Type::BYTE_ARRAY>;
template class value_decoder<format::Type::FIXED_LEN_BYTE_ARRAY>;

template <format::Type::type ParquetType>
class plain_encoder : public value_encoder<ParquetType>
{
   public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;

   private:
    std::vector<input_type> _buf;

   public:
    bytes_view view() const {
        const byte* data = reinterpret_cast<const byte*>(_buf.data());
        size_t size = _buf.size() * sizeof(input_type);
        return {data, size};
    }
    void put_batch(const input_type data[], size_t size) override { _buf.insert(_buf.end(), data, data + size); }
    size_t max_encoded_size() const override { return view().size(); }
    flush_result flush(byte sink[]) override {
        bytes_view v = view();
        std::copy(v.begin(), v.end(), sink);
        _buf.clear();
        return {v.size(), format::Encoding::PLAIN};
    }
    std::optional<bytes_view> view_dict() override { return {}; }
    uint64_t cardinality() override { return 0; }
};

template <>
class plain_encoder<format::Type::BYTE_ARRAY> : public value_encoder<format::Type::BYTE_ARRAY>
{
   public:
    using typename value_encoder<format::Type::BYTE_ARRAY>::input_type;
    using typename value_encoder<format::Type::BYTE_ARRAY>::flush_result;

   private:
    bytes _buf;

   private:
    void put(const input_type& str) {
        append_raw_bytes<uint32_t>(_buf, str.size());
        _buf.insert(_buf.end(), str.begin(), str.end());
    }

   public:
    bytes_view view() const { return {_buf.data(), _buf.size()}; }
    void put_batch(const input_type data[], size_t size) override {
        for (size_t i = 0; i < size; ++i) {
            put(data[i]);
        }
    }
    size_t max_encoded_size() const override { return _buf.size(); }
    flush_result flush(byte sink[]) override {
        std::copy(_buf.begin(), _buf.end(), sink);
        size_t size = _buf.size();
        _buf.clear();
        return {size, format::Encoding::PLAIN};
    }
    std::optional<bytes_view> view_dict() override { return {}; }
    uint64_t cardinality() override { return 0; }
};

template <>
class plain_encoder<format::Type::FIXED_LEN_BYTE_ARRAY> : public value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>
{
   public:
    using typename value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>::input_type;
    using typename value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>::flush_result;

   private:
    bytes _buf;

   private:
    void put(const input_type& str) { _buf.insert(_buf.end(), str.begin(), str.end()); }

   public:
    bytes_view view() const { return {_buf.data(), _buf.size()}; }
    void put_batch(const input_type data[], size_t size) override {
        for (size_t i = 0; i < size; ++i) {
            put(data[i]);
        }
    }
    size_t max_encoded_size() const override { return _buf.size(); }
    flush_result flush(byte sink[]) override {
        std::copy(_buf.begin(), _buf.end(), sink);
        size_t size = _buf.size();
        _buf.clear();
        return {size, format::Encoding::PLAIN};
    }
    std::optional<bytes_view> view_dict() override { return {}; }
    uint64_t cardinality() override { return 0; }
};

template <format::Type::type ParquetType>
class dict_builder
{
   public:
    using input_type = typename value_decoder_traits<ParquetType>::input_type;

   private:
    std::unordered_map<input_type, uint32_t> _accumulator;
    plain_encoder<ParquetType> _dict;

   public:
    uint32_t put(input_type key) {
        auto [iter, was_new_key] = _accumulator.try_emplace(key, _accumulator.size());
        if (was_new_key) {
            _dict.put_batch(&key, 1);
        }
        return iter->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return _dict.view(); }
};

template <>
class dict_builder<format::Type::BYTE_ARRAY>
{
   private:
    std::unordered_map<bytes, uint32_t, bytes_hasher> _accumulator;
    plain_encoder<format::Type::BYTE_ARRAY> _dict;

   public:
    uint32_t put(bytes_view key) {
        auto [it, was_new_key] = _accumulator.try_emplace(bytes{key}, _accumulator.size());
        if (was_new_key) {
            _dict.put_batch(&key, 1);
        }
        return it->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return _dict.view(); }
};

template <>
class dict_builder<format::Type::FIXED_LEN_BYTE_ARRAY>
{
   private:
    std::unordered_map<bytes, uint32_t, bytes_hasher> _accumulator;
    plain_encoder<format::Type::FIXED_LEN_BYTE_ARRAY> _dict;

   public:
    uint32_t put(bytes_view key) {
        auto [it, was_new_key] = _accumulator.try_emplace(bytes{key}, _accumulator.size());
        if (was_new_key) {
            _dict.put_batch(&key, 1);
        }
        return it->second;
    }
    size_t cardinality() const { return _accumulator.size(); }
    bytes_view view() const { return _dict.view(); }
};

template <format::Type::type ParquetType>
class dict_encoder : public value_encoder<ParquetType>
{
   private:
    std::vector<uint32_t> _indices;
    dict_builder<ParquetType> _values;

   private:
    int index_bit_width() const { return bit_width(_values.cardinality()); }

   public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;
    void put_batch(const input_type data[], size_t size) override {
        _indices.reserve(_indices.size() + size);
        for (size_t i = 0; i < size; ++i) {
            _indices.push_back(_values.put(data[i]));
        }
    }
    size_t max_encoded_size() const override {
        return 1 + RleEncoder::MinBufferSize(index_bit_width()) +
               RleEncoder::MaxBufferSize(index_bit_width(), _indices.size());
    }
    flush_result flush(byte sink[]) override {
        *sink = static_cast<byte>(index_bit_width());
        RleEncoder encoder{sink + 1, static_cast<int>(max_encoded_size() - 1), index_bit_width()};
        for (uint32_t index : _indices) {
            encoder.Put(index);
        }
        encoder.Flush();
        _indices.clear();
        size_t size = 1 + encoder.len();
        return {size, format::Encoding::RLE_DICTIONARY};
    }
    std::optional<bytes_view> view_dict() override { return _values.view(); }
    uint64_t cardinality() override { return _values.cardinality(); }
};

// Dict encoder, but it falls back to plain encoding
// when the dict page grows too big.
template <format::Type::type ParquetType>
class dict_or_plain_encoder : public value_encoder<ParquetType>
{
   private:
    dict_encoder<ParquetType> _dict_encoder;
    plain_encoder<ParquetType> _plain_encoder;
    bool fallen_back = false;  // Have we fallen back to plain yet?
   public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;
    // Will fall back to plain encoding when dict page grows
    // beyond this threshold.
    static constexpr size_t fallback_threshold = 16 * 1024;
    void put_batch(const input_type data[], size_t size) override {
        if (fallen_back) {
            _plain_encoder.put_batch(data, size);
        } else {
            _dict_encoder.put_batch(data, size);
        }
    }
    size_t max_encoded_size() const override {
        if (fallen_back) {
            return _plain_encoder.max_encoded_size();
        } else {
            return _dict_encoder.max_encoded_size();
        }
    }
    flush_result flush(byte sink[]) override {
        if (fallen_back) {
            return _plain_encoder.flush(sink);
        } else {
            if (_dict_encoder.view_dict()->size() > fallback_threshold) {
                fallen_back = true;
            }
            return _dict_encoder.flush(sink);
        }
    }
    std::optional<bytes_view> view_dict() override { return _dict_encoder.view_dict(); }
    uint64_t cardinality() override { return _dict_encoder.cardinality(); }
};

template <format::Type::type ParquetType>
struct arithmetic_type;

template <>
struct arithmetic_type<format::Type::INT32>
{
    using signed_type = int32_t;
    using unsigned_type = uint32_t;
};

template <>
struct arithmetic_type<format::Type::INT64>
{
    using signed_type = int64_t;
    using unsigned_type = uint64_t;
};

template <format::Type::type ParquetType>
class delta_binary_packed_encoder : public value_encoder<ParquetType>
{
    static_assert(ParquetType == format::Type::INT32 || ParquetType == format::Type::INT64);

   public:
    using typename value_encoder<ParquetType>::input_type;
    using typename value_encoder<ParquetType>::flush_result;
    using signed_type = typename arithmetic_type<ParquetType>::signed_type;
    using unsigned_type = typename arithmetic_type<ParquetType>::unsigned_type;

   private:
    static constexpr size_t BLOCK_VALUES = 256;
    static constexpr size_t MINIBLOCKS_PER_BLOCK = 8;
    static constexpr size_t VALUES_PER_MINIBLOCK = BLOCK_VALUES / MINIBLOCKS_PER_BLOCK;
    static constexpr size_t MAX_VLQ_BYTES = (ParquetType == format::Type::INT32) ? 5 : 10;
    size_t _total_values = 0;
    signed_type _first_value = 0;
    signed_type _last_value = 0;
    std::vector<signed_type> _unencoded_values;
    bytes _encoded_buffer;

   private:
    void flush_block() {
        if (_unencoded_values.empty()) {
            return;
        }

        unsigned_type deltas[BLOCK_VALUES];
        unsigned_type max_deltas[MINIBLOCKS_PER_BLOCK] = {};
        unsigned_type bit_widths[MINIBLOCKS_PER_BLOCK] = {};

        for (size_t i = 0; i < _unencoded_values.size(); ++i) {
            deltas[i] = static_cast<unsigned_type>(_unencoded_values[i]) - static_cast<unsigned_type>(_last_value);
            _last_value = _unencoded_values[i];
        }

        signed_type min_delta = deltas[0];
        for (size_t i = 0; i < _unencoded_values.size(); ++i) {
            // Implementation-defined behaviour!
            min_delta = std::min(min_delta, static_cast<signed_type>(deltas[i]));
        }
        for (size_t i = 0; i < _unencoded_values.size(); ++i) {
            deltas[i] = deltas[i] - static_cast<unsigned_type>(min_delta);
        }
        for (size_t i = 0; i < _unencoded_values.size(); ++i) {
            size_t miniblock = i / VALUES_PER_MINIBLOCK;
            max_deltas[miniblock] = std::max(max_deltas[miniblock], deltas[i]);
        }
        for (size_t mb = 0; mb < MINIBLOCKS_PER_BLOCK; ++mb) {
            bit_widths[mb] = bit_width(max_deltas[mb]);
        }

        size_t old_data_size = _encoded_buffer.size();
        size_t max_new_data_size = max_current_block_size();
        _encoded_buffer.resize(old_data_size + max_new_data_size);
        BitUtil::BitWriter _data_writer(&_encoded_buffer[old_data_size], max_new_data_size);

        _data_writer.PutZigZagVlqInt(min_delta);
        for (size_t mb = 0; mb < MINIBLOCKS_PER_BLOCK; ++mb) {
            _data_writer.PutAligned(bit_widths[mb], 1);
        }
        for (size_t mb = 0; mb < MINIBLOCKS_PER_BLOCK; ++mb) {
            size_t start_idx = mb * VALUES_PER_MINIBLOCK;
            size_t end_idx = (mb + 1) * VALUES_PER_MINIBLOCK;
            if (start_idx >= _unencoded_values.size()) {
                break;
            }
            for (size_t i = start_idx; i < end_idx; ++i) {
                _data_writer.PutValue(deltas[i], bit_widths[mb]);
            }
        }

        _data_writer.Flush();
        _unencoded_values.clear();
        _encoded_buffer.resize(old_data_size + _data_writer.bytes_written());
    }
    size_t max_current_block_size() const {
        size_t current_num_of_miniblocks = (_unencoded_values.size() + VALUES_PER_MINIBLOCK - 1) / VALUES_PER_MINIBLOCK;
        return MAX_VLQ_BYTES + MINIBLOCKS_PER_BLOCK +
               sizeof(input_type) * VALUES_PER_MINIBLOCK * current_num_of_miniblocks;
    }

   public:
    void put_batch(const input_type data[], size_t size) override {
        if (size == 0) {
            return;
        }

        size_t i = 0;
        if (__builtin_expect(_total_values == 0, false)) {
            _first_value = data[0];
            _last_value = _first_value;
            i = 1;
        }

        for (; i < size; ++i) {
            _unencoded_values.push_back(data[i]);
            if (_unencoded_values.size() == BLOCK_VALUES) {
                flush_block();
            }
        }

        _total_values += size;
    }
    size_t max_encoded_size() const override {
        constexpr size_t MAX_HEADER_SIZE = MAX_VLQ_BYTES * 4;
        return MAX_HEADER_SIZE + _encoded_buffer.size() + max_current_block_size();
    }
    flush_result flush(byte sink[]) override {
        flush_block();
        BitUtil::BitWriter header_writer(sink, max_encoded_size());
        header_writer.PutVlqInt(BLOCK_VALUES);
        header_writer.PutVlqInt(MINIBLOCKS_PER_BLOCK);
        header_writer.PutVlqInt(_total_values);
        header_writer.PutZigZagVlqInt(_first_value);
        header_writer.Flush();

        byte* data_pos = sink + header_writer.bytes_written();
        std::copy(_encoded_buffer.begin(), _encoded_buffer.end(), data_pos);
        _total_values = 0;
        _first_value = 0;
        _last_value = 0;
        size_t encoder_buffer_size = _encoded_buffer.size();
        _encoded_buffer.clear();
        return {encoder_buffer_size + header_writer.bytes_written(), format::Encoding::DELTA_BINARY_PACKED};
    }
};

template <format::Type::type ParquetType>
std::unique_ptr<value_encoder<ParquetType>> make_value_encoder(format::Encoding::type encoding) {
    if constexpr (ParquetType == format::Type::INT96) {
        throw parquet_exception("INT96 is deprecated and writes of this type are unsupported");
    }
    const auto not_implemented = [&]() {
        return parquet_exception(seastar::format("Encoding type {} as {} is not implemented yet",
                                                 static_cast<int32_t>(ParquetType), static_cast<int32_t>(encoding)));
    };
    const auto invalid = [&]() {
        return parquet_exception(seastar::format("Encoding {} is invalid for type {}", static_cast<int32_t>(encoding),
                                                 static_cast<int32_t>(ParquetType)));
    };
    if (encoding == format::Encoding::PLAIN) {
        return std::make_unique<plain_encoder<ParquetType>>();
    } else if (encoding == format::Encoding::PLAIN_DICTIONARY) {
        throw parquet_exception("PLAIN_DICTIONARY is deprecated. Use RLE_DICTIONARY instead");
    } else if (encoding == format::Encoding::RLE) {
        if constexpr (ParquetType == format::Type::BOOLEAN) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::BIT_PACKED) {
        throw invalid();
    } else if (encoding == format::Encoding::DELTA_BINARY_PACKED) {
        if constexpr (ParquetType == format::Type::INT32) {
            return std::make_unique<delta_binary_packed_encoder<ParquetType>>();
        }
        if constexpr (ParquetType == format::Type::INT64) {
            return std::make_unique<delta_binary_packed_encoder<ParquetType>>();
        }
        throw invalid();
    } else if (encoding == format::Encoding::DELTA_LENGTH_BYTE_ARRAY) {
        if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::DELTA_BYTE_ARRAY) {
        if constexpr (ParquetType == format::Type::BYTE_ARRAY) {
            throw not_implemented();
        }
        throw invalid();
    } else if (encoding == format::Encoding::RLE_DICTIONARY) {
        return std::make_unique<dict_or_plain_encoder<ParquetType>>();
    } else if (encoding == format::Encoding::BYTE_STREAM_SPLIT) {
        throw not_implemented();
    }
    throw parquet_exception(seastar::format("Unknown encoding ({})", static_cast<int32_t>(encoding)));
}

template std::unique_ptr<value_encoder<format::Type::INT32>> make_value_encoder<format::Type::INT32>(
  format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::INT64>> make_value_encoder<format::Type::INT64>(
  format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::FLOAT>> make_value_encoder<format::Type::FLOAT>(
  format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::DOUBLE>> make_value_encoder<format::Type::DOUBLE>(
  format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::BOOLEAN>> make_value_encoder<format::Type::BOOLEAN>(
  format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::BYTE_ARRAY>> make_value_encoder<format::Type::BYTE_ARRAY>(
  format::Encoding::type);
template std::unique_ptr<value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>>
  make_value_encoder<format::Type::FIXED_LEN_BYTE_ARRAY>(format::Encoding::type);

}  // namespace parquet4seastar
