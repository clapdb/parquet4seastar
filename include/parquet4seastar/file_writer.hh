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

#pragma once

#include <memory>
#include <parquet4seastar/column_chunk_writer.hh>
#include <parquet4seastar/writer_schema.hh>
#include <parquet4seastar/y_combinator.hh>
#include <ranges>
#include <seastar/core/seastar.hh>
#include <utility>

namespace parquet4seastar {

template <typename Class>
concept is_sink_v = requires(Class obj, const char* str, size_t len) {
    { obj.write(str, len) } -> std::same_as<seastar::future<>>;
    { obj.flush() } -> std::same_as<seastar::future<>>;
    { obj.close() } -> std::same_as<seastar::future<>>;
};

template <typename SINK>
requires is_sink_v<SINK>
class writer
{
   public:
    using column_chunk_writer_variant =
      std::variant<column_chunk_writer<format::Type::BOOLEAN>, column_chunk_writer<format::Type::INT32>,
                   column_chunk_writer<format::Type::INT64>, column_chunk_writer<format::Type::FLOAT>,
                   column_chunk_writer<format::Type::DOUBLE>, column_chunk_writer<format::Type::BYTE_ARRAY>,
                   column_chunk_writer<format::Type::FIXED_LEN_BYTE_ARRAY>>;

   private:
    bool _closed = false;
    SINK _sink;
    std::vector<column_chunk_writer_variant> _writers;
    format::FileMetaData _metadata;
    std::vector<std::vector<std::string>> _leaf_paths;
    thrift_serializer _thrift_serializer;
    size_t _file_offset = 0;

   private:
    void init_writers(const writer_schema::schema& root) {
        using namespace writer_schema;
        auto convert = y_combinator{[&](auto&& convert, const node& node_variant, uint32_t def, uint32_t rep) -> void {
            std::visit(
              overloaded{[&](const list_node& x) { convert(*x.element, def + 1 + x.optional, rep + 1); },
                         [&](const map_node& x) {
                             convert(*x.key, def + 1 + x.optional, rep + 1);
                             convert(*x.value, def + 1 + x.optional, rep + 1);
                         },
                         [&](const struct_node& x) {
                             for (const node& child : x.fields) {
                                 convert(child, def + x.optional, rep);
                             }
                         },
                         [&](const primitive_node& x) {
                             std::visit(
                               overloaded{
                                 [&](logical_type::INT96 logical_type) {
                                     throw parquet_exception("INT96 is deprecated. Writing INT96 is unsupported.");
                                 },
                                 [&](auto logical_type) {
                                     constexpr format::Type::type parquet_type = decltype(logical_type)::physical_type;
                                     writer_options options = {def + x.optional, rep, x.encoding, x.compression};
                                     _writers.push_back(make_column_chunk_writer<parquet_type>(options));
                                 }},
                               x.logical_type);
                         }},
              node_variant);
        }};
        for (const node& field : root.fields) {
            convert(field, 0, 0);
        }
    }

   public:
    explicit writer(SINK&& sink) : _sink(std::move(sink)) {}

    auto fetch_sink() -> SINK {
        assert(_closed);
        return std::move(_sink);
    }

    static seastar::future<std::unique_ptr<writer>> open_and_write_par1(SINK&& sink,
                                                                        const writer_schema::schema& schema) {
        auto fw = std::make_unique<writer>(std::move(sink));
        writer_schema::write_schema_result wsr = writer_schema::write_schema(schema);
        fw->_metadata.schema = std::move(wsr.elements);
        fw->_leaf_paths = std::move(wsr.leaf_paths);
        fw->init_writers(schema);
        fw->_file_offset = 4;
        co_await fw->_sink.write("PAR1", 4);
        co_return fw;
    }

    static seastar::future<std::unique_ptr<writer>> open(SINK&& sink, const writer_schema::schema& schema) {
        return seastar::futurize_invoke([&schema, &sink] { return open_and_write_par1(std::move(sink), schema); });
    }

    template <format::Type::type ParquetType>
    column_chunk_writer<ParquetType>& column(int i) {
        return std::get<column_chunk_writer<ParquetType>>(_writers[i]);
    }

    auto flush_page(int idx, uint64_t limit_size) -> bool {
        return std::visit(
          [&limit_size](auto& col) -> bool {
              if (col.current_page_max_size() > limit_size) {
                  col.flush_page();
                  return true;
              }
              return false;
          },
          _writers[idx]);
    }

    size_t estimated_row_group_size() const {
        size_t size = 0;
        for (const auto& writer : _writers) {
            std::visit([&](const auto& x) { size += x.estimated_chunk_size(); }, writer);
        }
        return size;
    }

    seastar::future<> flush_row_group() {
        using it = boost::counting_iterator<size_t>;

        _metadata.row_groups.push_back(format::RowGroup{});
        size_t rows_written = 0;
        if (_writers.size() > 0) {
            rows_written = std::visit([&](auto& x) { return x.rows_written(); }, _writers[0]);
        }
        _metadata.row_groups.rbegin()->__set_num_rows(rows_written);

        return seastar::do_for_each(it(0), it(_writers.size()), [this](size_t i) {
            return std::visit([&, i](auto& x) { return x.flush_chunk(_sink); }, _writers[i])
              .then([this, i](seastar::lw_shared_ptr<format::ColumnMetaData> cmd) {
                  cmd->dictionary_page_offset += _file_offset;
                  cmd->data_page_offset += _file_offset;
                  cmd->__set_path_in_schema(_leaf_paths[i]);
                  bytes_view footer = _thrift_serializer.serialize(*cmd);

                  _file_offset += cmd->total_compressed_size;
                  format::ColumnChunk cc;
                  cc.__set_file_offset(_file_offset);
                  cc.__set_meta_data(*cmd);
                  _metadata.row_groups.rbegin()->columns.push_back(cc);
                  _metadata.row_groups.rbegin()->__set_total_byte_size(_metadata.row_groups.rbegin()->total_byte_size +
                                                                       cmd->total_compressed_size + footer.size());

                  _file_offset += footer.size();
                  return _sink.write(reinterpret_cast<const char*>(footer.data()), footer.size());
              });
        });
    }

    seastar::future<> close() {
        _closed = true;
        return flush_row_group()
          .then([this] {
              for (const format::RowGroup& rg : _metadata.row_groups) {
                  _metadata.num_rows += rg.num_rows;
              }
              _metadata.__set_version(1);  // Parquet 2.0 == 1
              bytes_view footer = _thrift_serializer.serialize(_metadata);
              return _sink.write(reinterpret_cast<const char*>(footer.data()), footer.size()).then([this, footer] {
                  uint32_t footer_size = footer.size();
                  return _sink.write(reinterpret_cast<const char*>(&footer_size), 4);
              });
          })
          .then([this] { return _sink.write("PAR1", 4); })
          .then([this] { return _sink.flush(); })
          .then([this] { return _sink.close(); });
    }
};

template <typename Class>
concept is_sync_sink_v = requires(Class obj, const char* str, size_t len) {
    { obj.write(str, len) } -> std::same_as<void>;
    { obj.flush() } -> std::same_as<void>;
    { obj.close() } -> std::same_as<void>;
};

template <typename SINK>
requires is_sync_sink_v<SINK>
class sync_writer
{
   public:
    using column_chunk_writer_variant =
      std::variant<column_chunk_writer<format::Type::BOOLEAN>, column_chunk_writer<format::Type::INT32>,
                   column_chunk_writer<format::Type::INT64>, column_chunk_writer<format::Type::FLOAT>,
                   column_chunk_writer<format::Type::DOUBLE>, column_chunk_writer<format::Type::BYTE_ARRAY>,
                   column_chunk_writer<format::Type::FIXED_LEN_BYTE_ARRAY>>;

   private:
    bool _closed = false;
    SINK _sink;
    std::vector<column_chunk_writer_variant> _writers;
    format::FileMetaData _metadata;
    std::vector<std::vector<std::string>> _leaf_paths;
    thrift_serializer _thrift_serializer;
    size_t _file_offset = 0;

   private:
    void init_writers(const writer_schema::schema& root) {
        using namespace writer_schema;
        auto convert = y_combinator{[&](auto&& convert, const node& node_variant, uint32_t def, uint32_t rep) -> void {
            std::visit(
              overloaded{[&](const list_node& x) { convert(*x.element, def + 1 + x.optional, rep + 1); },
                         [&](const map_node& x) {
                             convert(*x.key, def + 1 + x.optional, rep + 1);
                             convert(*x.value, def + 1 + x.optional, rep + 1);
                         },
                         [&](const struct_node& x) {
                             for (const node& child : x.fields) {
                                 convert(child, def + x.optional, rep);
                             }
                         },
                         [&](const primitive_node& x) {
                             std::visit(
                               overloaded{
                                 [&](logical_type::INT96 logical_type) {
                                     throw parquet_exception("INT96 is deprecated. Writing INT96 is unsupported.");
                                 },
                                 [&](auto logical_type) {
                                     constexpr format::Type::type parquet_type = decltype(logical_type)::physical_type;
                                     writer_options options = {def + x.optional, rep, x.encoding, x.compression};
                                     _writers.push_back(make_column_chunk_writer<parquet_type>(options));
                                 }},
                               x.logical_type);
                         }},
              node_variant);
        }};
        for (const node& field : root.fields) {
            convert(field, 0, 0);
        }
    }

   public:
    explicit sync_writer(SINK&& sink) : _sink(std::move(sink)) {}

    auto fetch_sink() -> SINK {
        assert(_closed);
        return std::move(_sink);
    }

    static std::unique_ptr<sync_writer> open_and_write_par1(SINK&& sink, const writer_schema::schema& schema) {
        auto fw = std::make_unique<sync_writer>(std::move(sink));
        writer_schema::write_schema_result wsr = writer_schema::write_schema(schema);
        fw->_metadata.schema = std::move(wsr.elements);
        fw->_leaf_paths = std::move(wsr.leaf_paths);
        fw->init_writers(schema);
        fw->_file_offset = 4;
        fw->_sink.write("PAR1", 4);
        return fw;
    }

    static std::unique_ptr<sync_writer> open(SINK&& sink, const writer_schema::schema& schema) {
        return open_and_write_par1(std::move(sink), schema);
    }

    template <format::Type::type ParquetType>
    column_chunk_writer<ParquetType>& column(int i) {
        return std::get<column_chunk_writer<ParquetType>>(_writers[i]);
    }

    auto flush_page(int idx, uint64_t limit_size) -> bool {
        return std::visit(
          [&limit_size](auto& col) -> bool {
              if (col.current_page_max_size() > limit_size) {
                  col.flush_page();
                  return true;
              }
              return false;
          },
          _writers[idx]);
    }

    size_t estimated_row_group_size() const {
        size_t size = 0;
        for (const auto& writer : _writers) {
            std::visit([&](const auto& x) { size += x.estimated_chunk_size(); }, writer);
        }
        return size;
    }

    auto flush_row_group() -> void {
        _metadata.row_groups.push_back(format::RowGroup{});
        size_t rows_written = 0;
        if (_writers.size() > 0) {
            rows_written = std::visit([&](auto& x) { return x.rows_written(); }, _writers[0]);
        }
        _metadata.row_groups.rbegin()->__set_num_rows(rows_written);
        for (size_t i : std::ranges::iota_view(0U, _writers.size())) {
            auto cmd = std::visit([&](auto& x) { return x.sync_flush_chunk(_sink); }, _writers[i]);
            cmd->dictionary_page_offset += _file_offset;
            cmd->data_page_offset += _file_offset;
            cmd->__set_path_in_schema(_leaf_paths[i]);
            bytes_view footer = _thrift_serializer.serialize(*cmd);

            _file_offset += cmd->total_compressed_size;
            format::ColumnChunk cc;
            cc.__set_file_offset(_file_offset);
            cc.__set_meta_data(*cmd);
            _metadata.row_groups.rbegin()->columns.push_back(cc);
            _metadata.row_groups.rbegin()->__set_total_byte_size(_metadata.row_groups.rbegin()->total_byte_size +
                                                                 cmd->total_compressed_size + footer.size());
            _file_offset += footer.size();
            _sink.write(reinterpret_cast<const char*>(footer.data()), footer.size());
        }
    }

    auto close() -> void {
        _closed = true;
        flush_row_group();
        for (const format::RowGroup& rg : _metadata.row_groups) {
            _metadata.num_rows += rg.num_rows;
        }
        _metadata.__set_version(1);  // Parquet 2.0 == 1
        const bytes_view footer = _thrift_serializer.serialize(_metadata);
        _sink.write(reinterpret_cast<const char*>(footer.data()), footer.size());
        const uint32_t footer_size = footer.size();
        _sink.write(reinterpret_cast<const char*>(&footer_size), 4);
        _sink.write("PAR1", 4);
        _sink.flush();
        _sink.close();
    }
};

}  // namespace parquet4seastar
