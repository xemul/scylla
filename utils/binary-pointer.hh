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

#include <cassert>

namespace utils {

/*
 * A pointer that can point to either of two types. The exact
 * target is encoded into the least significant bit of the value
 * and all the values set are assert-ed not to have it.
 *
 * Additionally, a pointer can be 'notched' with yet another bit
 * for any user's purpose (spoiler: in RB-tree keys are painted
 * red/black with the notch on the parent pointer).
 */
template <typename A, typename B, bool Notched = false>
class binary_pointer {
    uintptr_t _v = 0;

    static constexpr uintptr_t _type_b_bit = 0x1;
    static constexpr uintptr_t _notch_bit = (Notched ? 0x2 : 0x0);
public:
    static constexpr uintptr_t pointer_mask = _type_b_bit | _notch_bit;

    /*
     * Run-time assertions on the pointer embarrass CPU branch predictor
     * and slow things down heavily, so the aligment checks are better to
     * be done by callers on the element allocation, and all subsequent
     * movements accross the tree go without it.
     */
    template <typename X>
    static void check_aligned(X* v) {
        assert(!(reinterpret_cast<uintptr_t>(v) & pointer_mask));
    };

private:
    template <typename X> struct type_select{};
    static constexpr uintptr_t bit(type_select<A>) noexcept { return 0; }
    static constexpr uintptr_t bit(type_select<B>) noexcept { return _type_b_bit; }

    /*
     * Mask to turn unwanted pointer into nullptr.
     * The mask(A) is all 1s if _v points to A and all zeroes
     * otherwise. The mask(B) is inverted.
     */
    uintptr_t mask(type_select<B>) const noexcept {
        return -(_v & _type_b_bit);
    }
    uintptr_t mask(type_select<A>) const noexcept {
        return ~mask(type_select<B>{});
    }

public:
    binary_pointer() noexcept = default;
    binary_pointer(const binary_pointer&) noexcept = default;
    binary_pointer(binary_pointer&&) noexcept = default;

    template <typename X>
    explicit binary_pointer(const X* v) noexcept
            : _v(reinterpret_cast<uintptr_t>(v) | bit(type_select<X>{})) { }

    void reset() noexcept { _v = 0; }

    template<typename X>
    bool is() const noexcept {
        return (_v & _type_b_bit) == bit(type_select<X>{});
    }

    template <typename X>
    X* as() const noexcept {
        /*
         * This is not just `_v & ~pointer_mask`, for the sake of femto-optimization.
         * For non-notched pointer calling as<A>() will just return the value without
         * making additional instructions.
         */
        return reinterpret_cast<X*>(_v & ~(bit(type_select<X>{}) | _notch_bit));
    }

    template <typename X>
    X* maybe_as() const noexcept {
        /*
         * This is the version of
         *
         *   return is<X>() ? as<X>() : nullptr
         *
         * that doesn't emit any conditional instructions and thus works
         * slightly faster.
         */
        return reinterpret_cast<X*>(_v & mask(type_select<X>{}) & ~pointer_mask);
    }

public:
    template <typename X>
    void set(const X* v) noexcept {
        uintptr_t rv = reinterpret_cast<uintptr_t>(v);
        _v = rv | bit(type_select<X>{}) | (_v & _notch_bit); // keep the notch
    }

    binary_pointer& operator=(const binary_pointer& o) noexcept {
        _v = o._v;
        return *this;
    }

    operator bool() const noexcept { return (_v & ~pointer_mask) != 0; }
    // Strict-aliasing is ON
    bool operator==(const binary_pointer& o) const noexcept { return _v == o._v; }

    bool notched() const noexcept { return _v & _notch_bit; }
    void notch() noexcept { _v |= _notch_bit; }
    void denotch() noexcept { _v &= ~_notch_bit; }
    void invert_notch() noexcept { _v ^= _notch_bit; }
};

} // namespace utils
