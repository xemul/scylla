/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

namespace utils {

static constexpr uint32_t uleb32_max_encoded_value = 0xffff;

static inline size_t uleb32_encoded_size(uint32_t val) noexcept {
    return log2floor(val) / 6;
}

template <typename Poison, typename Unpoison>
static inline void uleb32_encode(char*& pos, uint32_t val, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint64_t b = 64;
    auto start = pos;
    do {
        b |= val & 63;
        val >>= 6;
        if (!val) {
            b |= 128;
        }
        unpoison(pos, 1);
        *pos++ = b;
        b = 0;
    } while (val);
    poison(start, pos - start);
}

template <typename Poison, typename Unpoison>
static inline void uleb32_encode(char*& pos, uint32_t val, size_t encoded_size, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint64_t b = 64;
    auto start = pos;
    unpoison(start, encoded_size);
    do {
        b |= val & 63;
        val >>= 6;
        if (!--encoded_size) {
            b |= 128;
        }
        *pos++ = b;
        b = 0;
    } while (encoded_size);
    poison(start, pos - start);
}

template <typename Poison, typename Unpoison>
static inline uint32_t uleb32_decode_forwards(const char*& pos, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint32_t n = 0;
    unsigned shift = 0;
    auto p = pos; // avoid aliasing; p++ doesn't touch memory
    uint8_t b;
    do {
        unpoison(p, 1);
        b = *p++;
        if (shift < 32) {
            // non-canonical encoding can cause large shift; undefined in C++
            n |= uint32_t(b & 63) << shift;
        }
        shift += 6;
    } while ((b & 128) == 0);
    poison(pos, p - pos);
    pos = p;
    return n;
}

template <typename Poison, typename Unpoison>
static inline uint32_t uleb32_decode_bacwards(const char*& pos, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint32_t n = 0;
    uint8_t b;
    auto p = pos; // avoid aliasing; --p doesn't touch memory
    do {
        --p;
        unpoison(p, 1);
        b = *p;
        n = (n << 6) | (b & 63);
    } while ((b & 64) == 0);
    poison(p, pos - p);
    pos = p;
    return n;
}

} // namespace utils
