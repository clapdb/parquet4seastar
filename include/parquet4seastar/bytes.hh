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

#include <string>
#include <string_view>
#include <type_traits>
#include <cstdint>
#include <cstring>

namespace std {
template <>
struct char_traits<uint8_t> : public char_traits<char> {
    using char_type = uint8_t;
    using int_type = unsigned int;

    static void assign(char_type& r, const char_type& a) noexcept {
        r = a;
    }

    static char_type* assign(char_type* p, size_t count, char_type a) {
        for (size_t i = 0; i < count; ++i) {
            p[i] = a;
        }
        return p;
    }

    static constexpr bool eq(char_type a, char_type b) noexcept {
        return a == b;
    }

    static constexpr bool lt(char_type a, char_type b) noexcept {
        return a < b;
    }

    static char_type* move(char_type* dest, const char_type* src, size_t count) {
        if (count == 0) return dest;
        memmove(dest, src, count);
        return dest;
    }

    static char_type* copy(char_type* dest, const char_type* src, size_t count) {
        if (count == 0) return dest;
        memcpy(dest, src, count);
        return dest;
    }

    static constexpr int compare(const char_type* s1, const char_type* s2, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            if (lt(s1[i], s2[i])) return -1;
            if (lt(s2[i], s1[i])) return 1;
        }
        return 0;
    }

    static constexpr size_t length(const char_type* s) {
        size_t len = 0;
        while (!eq(s[len], char_type())) {
            ++len;
        }
        return len;
    }

    static constexpr const char_type* find(const char_type* s, size_t count, const char_type& ch) {
        for (size_t i = 0; i < count; ++i) {
            if (eq(s[i], ch)) return s + i;
        }
        return nullptr;
    }

    static constexpr char_type to_char_type(int_type c) noexcept {
        return static_cast<char_type>(c);
    }

    static constexpr int_type to_int_type(char_type c) noexcept {
        return static_cast<int_type>(c);
    }

    static constexpr bool eq_int_type(int_type c1, int_type c2) noexcept {
        return c1 == c2;
    }

    static constexpr int_type eof() noexcept {
        return static_cast<int_type>(-1);
    }

    static constexpr int_type not_eof(int_type e) noexcept {
        return eq_int_type(e, eof()) ? 0 : e;
    }
};
} // namespace std

namespace parquet4seastar {

using bytes = std::basic_string<uint8_t>;
using bytes_view = std::basic_string_view<uint8_t>;
using byte = bytes::value_type;

struct bytes_hasher {
    size_t operator()(const bytes &s) const {
        return std::hash<std::string_view>{}(std::string_view{reinterpret_cast<const char *>(s.data()), s.size()});
    }
};

template<typename T, typename = std::enable_if_t<std::is_trivially_copyable_v<T>>>
void append_raw_bytes(bytes &b, T v) {
    const byte *data = reinterpret_cast<const byte *>(&v);
    b.insert(b.end(), data, data + sizeof(v));
}

} // namespace parquet4seastar
