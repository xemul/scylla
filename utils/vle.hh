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

/*
 * For ASAN case poisoning helpers are not-trivial and
 * using optimized encoder doesn't make noticeable profit
 */
#if defined(__x86_64__) && !defined(SEASTAR_ASAN_ENABLED)

#ifdef __ARCH_HAS_FAST_PDEP__
#include <x86intrin.h>
/*
 * AMD chips have this instruction implemented in microcode which
 * gives HUUUUGE performance penalty. On Intel this would give us
 * ~10% more performance.
 */
static inline uint32_t _uleb32_expand(uint32_t val) noexcept {
    static_assert(uleb32_max_encoded_value <= (1 << 24));
    return _pdep_u32(val, 0x3f3f3f3f);
}
#else
static inline uint32_t _uleb32_expand(uint32_t val) noexcept {
    uint32_t ret = val & 0x3f;
    if constexpr (uleb32_max_encoded_value > 0x3f) {
        ret |= (val & 0xfc0) << 2;
        if constexpr (uleb32_max_encoded_value > 0xfc0) {
            ret |= (val & 0x3f000) << 4;
            if constexpr (uleb32_max_encoded_value > 0x3f000) {
                ret |= (val & 0xfc0000) << 6;
                static_assert(uleb32_max_encoded_value <= 0xfc0000);
            }
        }
    }
    return ret;
}
#endif

template <typename Poison, typename Unpoison>
static inline void uleb32_encode(char*& pos, uint32_t val, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint32_t res = _uleb32_expand(val) | 64;
    /*
     * The _bit_scan_reverse is slightly better, but needs
     * the -mbmi2 flag to compile
     */
    size_t encoded_size = (32 - __builtin_clz(res)) >> 3;
    res |= 128 << (encoded_size << 3);
    /*
     * This version is used to fill trailing gaps on segments
     * so mind the encoded_size -- there can be no available
     * memory after it
     */
    memcpy(pos, &res, encoded_size + 1);
    pos += encoded_size + 1;
}

template <typename Poison, typename Unpoison>
static inline void uleb32_encode(char*& pos, uint32_t val, size_t encoded_size, Poison&& poison, Unpoison&& unpoison) noexcept {
    /*
     * Most, if not all, of the allocations in LSA come with
     * alignment <= sizeof(pointer) so this gap can only exceed
     * 8 bytes in one case -- if the previous allocation ended
     * up with 1-byte gap and the encoder says it needs 2+ for
     * the descriptor, then we'll get a 1 + sizeof(pointer) =
     * = 9-byte gap to fill.
     *
     * Also after the descriptor there _will_ come an object,
     * so we _do_ have at least 8 bytes of memory available.
     *
     * Said that in a typicall situation the steps are
     *  - put the zero at the trailing byte. If it's less than the
     *    9'th byte it will be overwritten
     *  - put 8 bytes of the encoded value. This possibly overwrites
     *    the object's memory, but the object is to be allocated in
     *    it, so OK
     *  - bump the "ending" bit in the trailing byte
     *
     * This works for gaps of sizes 1 through 9. But just in case
     * someone uses larger alignment, we need to fill the wider
     * gap with zeroes
     */

    uint64_t res = _uleb32_expand(val) | 64;
    if (encoded_size > sizeof(uint64_t) + 1) [[unlikely]] {
        memset(pos + sizeof(uint64_t), 0, encoded_size - (sizeof(uint64_t) + 1));
    }
    pos[encoded_size - 1] = 0x0;
    *reinterpret_cast<uint64_t*>(pos) = res;
    pos[encoded_size - 1] |= 128;
    pos += encoded_size;
}
#else
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
#endif

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
