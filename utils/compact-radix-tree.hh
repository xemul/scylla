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
#include <algorithm>
#include <bitset>
#include <fmt/core.h>
#include "utils/allocation_strategy.hh"
#include "utils/array-search.hh"

class size_calculator;

namespace compact_radix_tree {

template <typename T, typename Idx> class printer;

template <unsigned Size>
inline unsigned find_in_array(uint8_t val, const uint8_t* arr);

template <>
inline unsigned find_in_array<4>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_4_eq(val, arr);
}

template <>
inline unsigned find_in_array<16>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_16_eq(val, arr);
}

// A union of any number of types.
template <bool, typename...> struct variadic_union;
template <bool Sentinel> struct variadic_union<Sentinel> {};

template <bool Sentinel, typename Tx, typename... Ts>
struct variadic_union<Sentinel, Tx, Ts...> {
    union {
        Tx _this;
        variadic_union<Sentinel, Ts...> _other;
    };

    variadic_union() noexcept {}
    ~variadic_union() {}
};

// Concepts for nodes layouts (see layouts comments)
template <typename T>
concept HasSlots = requires (T d) { { d._slots[0] }; };

template <typename T>
concept OptionalSlots = requires (T d, unsigned idx) {
    { d.set(idx) } noexcept -> std::same_as<void>;
    { d.has(idx) } noexcept -> std::same_as<bool>;
    { d.unset(idx) } noexcept -> std::same_as<void>;
};

template <typename T, typename Index = unsigned int>
requires std::is_nothrow_move_constructible_v<T> && std::is_integral_v<Index>
class tree {
    template <typename A, typename I> friend class printer;
    friend class ::size_calculator;

    class leaf_node;
    class inner_node;
    struct node_head;
    class node_head_ptr;

public:
    /*
     * The search key in the tree is an integer, the whole
     * logic below is optimized for that.
     */
    using index_t = std::make_unsigned_t<Index>;

    /*
     * The lookup uses individual bytes from the key to search on
     * each level. Thus all levels but the last one keep pointers
     * on lower levels, the lsat one is the leaf node that keeps
     * values on board.
     */
    using node_index_t = uint8_t;
    static constexpr unsigned leaf_depth = sizeof(index_t) - 1;
    static constexpr unsigned node_index_limit = 256;

private:
    /*
     * One each level node can have one of 4 different layouts,
     * depending on the amount of "used" indices on it. When a
     * node is tried to push a new index over its current capacity
     * it grows into the next layout. When a key is removed from
     * a node and it becomes less that some threshold, it's shrunk
     * into previous layout.
     *
     * Respectively, the node starts with the tiny layout and the
     * large layout should be capable to carry node_index_limit
     * indices on board.
     *
     * The nil kind is special empty node into which empty tree
     * points not to perform extra nullptr check on lookup.
     */
    enum class node_kind : uint8_t { nil, tiny, small, medium, large, };

    /*
     * When a node has only one child, the former is removed from
     * the tree and its parent is set up to directly point to this
     * only kid. The kid, in turn, carries a "prefix" on board
     * denoting the index that might have been skipped by this cut.
     *
     * The top 3 bytes are the index itself, the lower byte is the
     * number of skipped levels.
     */
    static constexpr index_t prefix_len_mask = 0xff;
    static constexpr index_t prefix_mask = ((index_t)-1) << 8;

    static index_t node_prefix(index_t idx, unsigned len) noexcept {
        return (idx & prefix_mask) + len;
    }

    /*
     * Mask to check nove's prefix (mis-)match
     */
    static index_t prefix_mask_at(unsigned depth) noexcept {
        return prefix_mask << (8 * (leaf_depth - depth));
    }

    /*
     * Constructs an index from (leaf) node prefix and value
     * index on a node
     */
    static index_t tree_index(index_t prefix, node_index_t idx) noexcept {
        return (prefix & prefix_mask) + idx;
    }

    /*
     * Finds the number of leading bytes that coinside for two
     * indices. Needed on insertion, when a short-cut node gets
     * expanded back.
     */
    static unsigned common_prefix_len(index_t i1, index_t i2) noexcept {
        /*
         * This won't work if i1 == i2 (clz is undefined for full
         * zeroes value), but we don't get here in this case
         */
        return __builtin_clz(i1 ^ i2) / 8;
    }

    /*
     * Gets the depth's 8-bit index from the 32-bit one, that's used
     * in intra-node search. The leaf-level index is the 0th byte, so
     * that sequential indices naturally pack into one node.
     */
    static node_index_t node_index(index_t idx, unsigned depth) noexcept {
        return (idx >> (8 * (leaf_depth - depth))) & 0xff;
    }

    enum class erase_mode { real, cleanup, };

    /*
     * When removing an index from a node it may end-up in one of 4
     * states:
     *
     * - empty  -- the last index was removed, the parent node is
     *             welcome to drop the slot and mark it as unused
     *             (and maybe get shrunk/squashed after that)
     * - squash -- only one index left, the parent node is welcome
     *             to remove this node and replace it with its only
     *             child (tuning it's prefix respectively)
     * - shrink -- current layout contains few indices, so parent
     *             node should shrink the slot into previous layout
     * - nothing - just nothing
     */
    enum class erase_result { nothing, empty, shrink, squash, };

    static erase_result after_drop(unsigned count, unsigned shrink_threshold) noexcept {
        if (count == 0) {
            return erase_result::empty;
        }
        if (count == 1) {
            return erase_result::squash;
        }
        if (count <= shrink_threshold) {
            return erase_result::shrink;
        }

        return erase_result::nothing;
    }

    /*
     * Lower-bound calls return back pointer on the value and
     * the leaf node_head on which the value was found. The latter
     * is needed for iterator's ++ optimization.
     */
    using lower_bound_res = std::pair<const T*, const node_head*>;

    /*
     * Allocation returns a slot pointer and a boolean denoting
     * if the allocation really took place (false if the slot
     * is aleady occupied)
     */
    using allocate_res = std::pair<T*, bool>;

    /*
     * Node layouts. Define the way indices and payloads are stored on the
     * node. Inner and leaf nodes have different ways of maintaining the
     * values vs pointers on nodes, thus the layouts accept the storage
     * as template argument.
     */

    /*
     * In table layout the node looks like a list of (index, payload) pairs.
     * The intra-node search is to loop through the list fining the needed
     * index, then return back the corresponding value.
     *
     * Suitable for small number of indices.
     */
    template <typename Table>
    requires HasSlots<Table> && OptionalSlots<Table>
    struct table_layout_base {
        using slot_type = typename Table::slot_type;

        Table _data;

        void construct() noexcept { _data.construct(); }

        void construct(table_layout_base&& o) noexcept {
            _data.construct(std::move(o._data));
        }

        const slot_type* find(node_index_t idx) const noexcept {
            /*
             * The array_search_xxx result is undefined if the idx is
             * not there, but that's OK as data.has() check is here
             */
            unsigned i = find_in_array<Table::size>(idx, _data._idx);
            return i < Table::size && _data.has(i) ? &_data._slots[i] : nullptr;
        }

        const slot_type* pop(node_index_t& idx) const noexcept {
            for (unsigned i = 0; i < Table::size; i++) {
                if (_data.has(i)) {
                    idx = i;
                    return &_data._slots[i];
                }
            }

            return nullptr;
        }

        /*
         * Reserving an index in a table means -- finding an empty
         * slot or the occupied one with the matching index. While
         * scanning -- tries to find an unused slot if the index in
         * question is not there.
         */
        allocate_res alloc(index_t idx, unsigned depth) {
            node_index_t ni = node_index(idx, depth);

            unsigned free = Table::size;
            for (unsigned i = 0; i < Table::size; i++) {
                if (_data.has(i)) {
                    if (_data._idx[i] == ni) {
                        return allocate_on(&_data._slots[i], idx, depth + 1, false);
                    }
                } else if (free == Table::size) {
                    free = i;
                }
            }

            if (free == Table::size) {
                return allocate_res(nullptr, false);
            }

            populate_slot(&_data._slots[free], idx, depth);
            _data._idx[free] = ni;
            _data.set(free);

            return allocate_on(&_data._slots[free], idx, depth + 1, true);
        }

        slot_type* reserve(node_index_t idx) noexcept {
            for (unsigned i = 0; i < Table::size; i++) {
                if (!_data.has(i)) {
                    _data._idx[i] = idx;
                    _data.set(i);
                    return &_data._slots[i];
                }
            }
            std::abort();
        }

        void erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            node_index_t ni = node_index(idx, depth);

            for (unsigned i = 0; i < Table::size; i++) {
                if (_data._idx[i] == ni && _data.has(i)) {
                    if (erase_from_slot(&_data._slots[i], idx, depth + 1, erm)) {
                        _data.unset(i);
                    }
                    break;
                }
            }
        }

        template <typename Fn>
        void weed(Fn&& filter, index_t pfx, unsigned depth) {
            for (unsigned i = 0; i < Table::size; i++) {
                if (_data.has(i)) {
                    if (weed_from_slot(pfx, _data._idx[i], &_data._slots[i], filter, depth + 1)) {
                        _data.unset(i);
                    }
                }
            }
        }

        void grow(auto& into) noexcept {
            for (unsigned i = 0; i < Table::size; i++) {
                assert(_data.has(i));
                move_slot(std::move(_data._slots[i]), into, _data._idx[i]);
            }
        }

        lower_bound_res lower_bound(index_t& idx, index_t pfx, unsigned depth) const noexcept {
            node_index_t ni = node_index(idx, depth);
            unsigned ui = Table::size;

            for (unsigned i = 0; i < Table::size; i++) {
                if (_data.has(i)) {
                    if (_data._idx[i] == ni) {
                        lower_bound_res ret = lower_bound_at(pfx, _data._idx[i], &_data._slots[i], depth, idx);
                        if (ret.first != nullptr) {
                            return ret;
                        }
                    } else if (_data._idx[i] > ni) {
                        if (ui == Table::size || _data._idx[i] < _data._idx[ui]) {
                            ui = i;
                        }
                    }
                }
            }

            if (ui == Table::size) {
                return lower_bound_res(nullptr, nullptr);
            }

            /*
             * Nothing was found on the slot, that matches the
             * given index. We need to move to the next one, but
             * zero-out all idx's bits related to lower levels.
             *
             * Fortunately, leaf nodes will rewrite the whole
             * thing on match, so put 0 into the whole idx.
             *
             * Also note, that short-cut iterator++ assumes that
             * index is NOT 0-ed in case of mismatch!
             */
            idx = 0;
            return lower_bound_at(pfx, _data._idx[ui], &_data._slots[ui], depth, idx);
        }

        template <typename Visitor>
        bool visit(Visitor&& v, index_t pfx, unsigned depth) const {
            unsigned indices[Table::size];
            unsigned sz = 0;

            for (unsigned i = 0; i < Table::size; i++) {
                if (_data.has(i)) {
                    indices[sz++] = i;
                }
            }

            if (v.sorted) {
                std::sort(indices, indices + sz, [this] (int a, int b) {
                    return _data._idx[a] < _data._idx[b];
                });
            }

            for (unsigned i = 0; i < sz; i++) {
                unsigned pos = indices[i];
                if (!visit_slot(v, pfx, _data._idx[pos], &_data._slots[pos], depth)) {
                    return false;
                }
            }
            return true;
        }
    };

    /*
     * The table layout is used two times -- one as the tiny one, and
     * then by the small one. Repspectively, tiny one is unshrinkable,
     * while the small one -- is.
     */
    template <typename Table, node_kind Kind>
    struct minimal_table_layout : public table_layout_base<Table> {
        static constexpr unsigned size = Table::size;
        static constexpr node_kind kind = Kind;

        using base = table_layout_base<Table>;

        erase_result erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            base::erase(idx, depth, erm);
            return base::_data.after_drop_no_shrink();
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, index_t pfx, unsigned depth) {
            base::weed(filter, pfx, depth);
            return base::_data.after_drop_no_shrink();
        }

        void shrink(auto& into) noexcept {
            __builtin_unreachable();
        }
    };

    template <typename Table, node_kind Kind, unsigned ShrinkThreshold>
    struct table_layout : public table_layout_base<Table> {
        static constexpr unsigned size = Table::size;
        static constexpr node_kind kind = Kind;

        using base = table_layout_base<Table>;

        void shrink(auto& into) noexcept {
            unsigned count = 0;
            for (unsigned i = 0; i < Table::size; i++) {
                if (!base::_data.has(i)) {
                    continue;
                }

                move_slot(std::move(base::_data._slots[i]), into, base::_data._idx[i]);
                if (++count == ShrinkThreshold) {
                    break;
                }
            }
        }

        erase_result erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            base::erase(idx, depth, erm);
            return after_drop(base::_data.count(), ShrinkThreshold);
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, index_t pfx, unsigned depth) {
            base::weed(filter, pfx, depth);
            return after_drop(base::_data.count(), ShrinkThreshold);
        }
    };

    /*
     * Map layout keeps the node_index_limit-sized map of internal indices
     * and a smaller array of data to which the mapped indices point to.
     * The lookup is data[map[index]]. Good for sizes that are too large for
     * linear/binary scan, but yet small for the real array with holes.
     */
    template <typename Map, node_kind Kind, unsigned ShrinkThreshold>
    requires HasSlots<Map>
    struct map_layout {
        static constexpr node_kind kind = Kind;
        static constexpr unsigned size = Map::size;
        static_assert(Map::index_size == node_index_limit);
        using slot_type = typename Map::slot_type;
        static constexpr uint8_t unused = size;

        Map _data;

        /*
         * The storage's _slots is re-used to maintain single-linked
         * freelist of indices for O(1) free slot allocation.
         */
        uint8_t* raw_slot(unsigned i) noexcept {
            return reinterpret_cast<uint8_t*>(&_data._slots[i]);
        }

        void construct() noexcept {
            for (unsigned i = 0; i < Map::index_size; i++) {
                _data._map[i] = unused;
            }
            _data._free_mi = 0;
            _data._used_num = 0;
            for (unsigned i = 0; i < Map::size; i++) {
                *raw_slot(i) = i + 1;
            }
        }

        void construct(map_layout&& o) noexcept {
            for (unsigned i = 0; i < Map::index_size; i++) {
                uint8_t mi = _data._map[i] = o._data._map[i];
                if (mi != unused) {
                    move_slot(std::move(o._data._slots[mi]), &_data._slots[mi]);
                }
            }

            uint8_t mi = _data._free_mi = o._data._free_mi;
            _data._used_num = o._data._used_num;

            while (mi != unused) {
                uint8_t t = *raw_slot(mi) = *o.raw_slot(mi);
                mi = t;
            }
        }

        const slot_type* find(node_index_t i) const noexcept {
            uint8_t mi = _data._map[i];
            return mi == unused ? nullptr : &_data._slots[mi];
        }

        const slot_type* pop(node_index_t& idx) const noexcept {
            for (unsigned i = 0; i < Map::index_size; i++) {
                uint8_t mi = _data._map[i];
                if (mi != unused) {
                    idx = mi;
                    return &_data._slots[mi];
                }
            }

            return nullptr;
        }

        allocate_res alloc(index_t idx, unsigned depth) {
            node_index_t ni = node_index(idx, depth);

            uint8_t mi = _data._map[ni];
            if (mi != unused) {
                return allocate_on(&_data._slots[mi], idx, depth + 1, false);
            }

            if (_data._free_mi == unused) {
                assert(_data._used_num == Map::size);
                return allocate_res(nullptr, false);
            }

            assert(_data._used_num < Map::size);
            mi = _data._free_mi;
            uint8_t next = *raw_slot(mi);
            populate_slot(&_data._slots[mi], idx, depth);
            _data._free_mi = next;
            _data._used_num++;
            _data._map[ni] = mi;

            return allocate_on(&_data._slots[mi], idx, depth + 1, true);
        }

        slot_type* reserve(node_index_t i) noexcept {
            uint8_t mi = _data._free_mi;
            assert(_data._used_num < Map::size && mi != unused);
            _data._free_mi = *raw_slot(mi);
            _data._used_num++;
            _data._map[i] = mi;
            return &_data._slots[mi];
        }

        void free_slot(unsigned pos, uint8_t mi) noexcept {
            *raw_slot(mi) = _data._free_mi;
            _data._free_mi = mi;
            _data._used_num--;
            _data._map[pos] = unused;
        }

        erase_result erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            node_index_t ni = node_index(idx, depth);
            uint8_t mi = _data._map[ni];

            if (mi != unused) {
                if (erase_from_slot(&_data._slots[mi], idx, depth + 1, erm)) {
                    assert(_data._used_num > 0);
                    free_slot(ni, mi);
                    return after_drop(_data._used_num, ShrinkThreshold);
                }
            }

            return erase_result::nothing;
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, index_t pfx, unsigned depth) {
            for (unsigned i = 0; i < Map::index_size; i++) {
                uint8_t mi = _data._map[i];
                if (mi != unused) {
                    if (weed_from_slot(pfx, i, &_data._slots[mi], filter, depth + 1)) {
                        free_slot(i, mi);
                    }
                }
            }
            return after_drop(_data._used_num, ShrinkThreshold);
        }

        template <typename TN>
        void relocate(TN& into, unsigned limit) noexcept {
            unsigned count = 0;
            for (unsigned i = 0; i < Map::index_size; i++) {
                uint8_t mi = _data._map[i];
                if (mi != unused) {
                    move_slot(std::move(_data._slots[mi]), into, i);
                    count++;
                    if (count == limit) {
                        break;
                    }
                }
            }
        }

        void grow(auto& into) noexcept { relocate(into, Map::size); }
        void shrink(auto& into) noexcept { relocate(into, ShrinkThreshold); }

        lower_bound_res lower_bound(index_t& idx, index_t pfx, unsigned depth) const noexcept {
            node_index_t ni = node_index(idx, depth);

            uint8_t mi = _data._map[ni];
            if (mi != unused) {
                lower_bound_res ret = lower_bound_at(pfx, ni, &_data._slots[mi], depth, idx);
                if (ret.first != nullptr) {
                    return ret;
                }
            }

            for (unsigned i = ni + 1; i < Map::index_size; i++) {
                mi = _data._map[i];
                if (mi != unused) {
                    // See comment in table_layout_base about this 0
                    idx = 0;
                    return lower_bound_at(pfx, i, &_data._slots[mi], depth, idx);
                }
            }

            return lower_bound_res(nullptr, nullptr);
        }

        template <typename Visitor>
        bool visit(Visitor&& v, index_t pfx, unsigned depth) const {
            for (unsigned i = 0; i < Map::index_size; i++) {
                uint8_t mi = _data._map[i];
                if (mi != unused) {
                    if (!visit_slot(v, pfx, i, &_data._slots[mi], depth)) {
                        return false;
                    }
                }
            }
            return true;
        }
    };

    /*
     * Array layout is just an array of data. The storage needs to be able
     * to answer the .has(i) question -- whether or not the respective slot
     * is used or not.
     *
     * Suitable for the large layout (only).
     */
    template <typename Array, node_kind Kind, unsigned ShrinkThreshold>
    requires HasSlots<Array> && OptionalSlots<Array>
    struct array_layout {
        static_assert(Array::size == node_index_limit);
        static constexpr node_kind kind = Kind;

        using slot_type = typename Array::slot_type;

        Array _data;

        void construct() noexcept { _data.construct(); }

        void construct(array_layout&& o) noexcept {
            _data.construct(std::move(o._data));
        }

        const slot_type* find(node_index_t idx) const noexcept {
            return _data.has(idx) ? &_data._slots[idx] : nullptr;
        }

        const slot_type* pop(node_index_t& idx) const noexcept {
            for (unsigned i = 0; i < Array::size; i++) {
                if (_data.has(i)) {
                    idx = i;
                    return &_data._slots[i];
                }
            }

            return nullptr;
        }

        allocate_res alloc(index_t idx, unsigned depth) {
            node_index_t ni = node_index(idx, depth);

            if (_data.has(ni)) {
                return allocate_on(&_data._slots[ni], idx, depth + 1, false);
            }

            populate_slot(&_data._slots[ni], idx, depth);
            _data.set(ni);

            return allocate_on(&_data._slots[ni], idx, depth + 1, true);
        }

        slot_type* reserve(node_index_t idx) noexcept {
            assert(!_data.has(idx));
            _data.set(idx);
            return &_data._slots[idx];
        }

        erase_result erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            node_index_t ni = node_index(idx, depth);

            if (_data.has(ni)) {
                if (erase_from_slot(&_data._slots[ni], idx, depth + 1, erm)) {
                    _data.unset(ni);
                    unsigned count = _data.count();
                    return after_drop(count, ShrinkThreshold);
                }
            }

            return erase_result::nothing;
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, index_t pfx, unsigned depth) {
            for (unsigned i = 0; i < Array::size; i++) {
                if (_data.has(i)) {
                    if (weed_from_slot(pfx, i, &_data._slots[i], filter, depth + 1)) {
                        _data.unset(i);
                    }
                }
            }
            return after_drop(_data.count(), ShrinkThreshold);
        }

        void shrink(auto& into) noexcept {
            unsigned count = 0;
            for (unsigned i = 0; i < Array::size; i++) {
                if (_data.has(i)) {
                    move_slot(std::move(_data._slots[i]), into, i);
                    count++;
                    if (count == ShrinkThreshold) {
                        break;
                    }
                }
            }
        }

        lower_bound_res lower_bound(index_t& idx, index_t pfx, unsigned depth) const noexcept {
            node_index_t ni = node_index(idx, depth);

            if (_data.has(ni)) {
                lower_bound_res ret = lower_bound_at(pfx, ni, &_data._slots[ni], depth, idx);
                if (ret.first != nullptr) {
                    return ret;
                }
            }

            for (unsigned i = ni + 1; i < Array::size; i++) {
                if (_data.has(i)) {
                    // See comment in table_layout_base about this 0
                    idx = 0;
                    return lower_bound_at(pfx, i, &_data._slots[i], depth, idx);
                }
            }

            return lower_bound_res(nullptr, nullptr);
        }

        template <typename Visitor>
        bool visit(Visitor&& v, index_t pfx, unsigned depth) const {
            for (unsigned i = 0; i < Array::size; i++) {
                if (_data.has(i)) {
                    if (!visit_slot(v, pfx, i, &_data._slots[i], depth)) {
                        return false;
                    }
                }
            }
            return true;
        }
    };

    /*
     * Table of values. Slots are T-s, presece of a slot is tracked
     * with a short bitset
     */
    template <unsigned Size, typename Bitset>
    struct table_of_values {
        static_assert(sizeof(Bitset) * 8 >= Size);
        static constexpr unsigned size = Size;
        using slot_type = T;

        /*
         * std::bitset is 8 bytes minimum, but for our
         * tiny/small layouts we might need only 1 or 2
         */
        Bitset _present;
        node_index_t _idx[Size];
        T _slots[Size];

        void construct() noexcept { _present = 0; }
        void construct(table_of_values&& o) noexcept {
            for (unsigned i = 0; i < Size; i++) {
                _idx[i] = o._idx[i];
                if (o.has(i)) {
                    move_slot(std::move(o._slots[i]), &_slots[i]);
                }
            }
            _present = o._present;
            o._present = 0;
        }

        bool has(unsigned i) const noexcept { return _present & (1 << i); }
        void set(unsigned i) noexcept { _present |= (1 << i); }
        void unset(unsigned i) noexcept { _present &= ~(1 << i); }
        unsigned count() const noexcept { return __builtin_popcount(_present); }

        erase_result after_drop_no_shrink() const noexcept {
            return _present == 0 ? erase_result::empty : erase_result::nothing;
        }
    };

    /*
     * Map of values. Again, slots are T-s and no map-specific stuff here
     */
    template <unsigned Size>
    struct map_of_values {
        static constexpr unsigned index_size = node_index_limit;
        static constexpr unsigned size = Size;
        using slot_type = T;

        uint8_t _free_mi, _used_num;
        uint8_t _map[index_size];
        T _slots[size];
    };

    /*
     * Array of values. Like table of values, but with a large bitset
     * and without indexes
     */
    struct array_of_values {
        static constexpr unsigned size = node_index_limit;
        using slot_type = T;

        std::bitset<size> _present;
        T _slots[size];

        void construct() noexcept {
            _present.reset();
        }

        void construct(array_of_values&& o) noexcept {
            for (unsigned i = 0; i < size; i++) {
                if (o.has(i)) {
                    move_slot(std::move(o._slots[i]), &_slots[i]);
                }
            }
            _present = std::move(o._present);
            o._present.reset();
        }

        bool has(unsigned i) const noexcept { return _present.test(i); }
        void set(unsigned i) noexcept { _present.set(i); }
        void unset(unsigned i) noexcept { _present.set(i, false); }
        unsigned count() const noexcept { return _present.count(); }
    };

    /*
     * Table of pointers on lower nodes. Presence is tracked naturally
     * by checking the pointer for nullptr
     */
    template <unsigned Size>
    struct table_of_node_ptrs {
        static constexpr unsigned size = Size;
        using slot_type = node_head_ptr;

        node_index_t _idx[Size];
        node_head_ptr _slots[Size];

        void construct() noexcept {
            for (unsigned i = 0; i < Size; i++) {
                _slots[i] = nullptr;
            }
        }
        void construct(table_of_node_ptrs&& o) noexcept {
            for (unsigned i = 0; i < Size; i++) {
                _idx[i] = o._idx[i];
                move_slot(std::move(o._slots[i]), &_slots[i]);
            }
        }

        bool has(unsigned i) const noexcept { return _slots[i]; }
        void set(unsigned i) noexcept {}
        void unset(unsigned i) noexcept {}

        unsigned count() const noexcept {
            unsigned ret = 0;
            for (unsigned i = 0; i < Size; i++) {
                if (_slots[i]) {
                    ret++;
                }
            }
            return ret;
        }

        erase_result after_drop_no_shrink() const noexcept {
            switch (count()) {
            case 0:
                return erase_result::empty;
            case 1:
                return erase_result::squash;
            default:
                return erase_result::nothing;
            }
        }
    };

    /*
     * Map of pointers on lower nodes
     */
    template <unsigned Size>
    struct map_of_node_ptrs {
        static constexpr unsigned index_size = node_index_limit;
        static constexpr unsigned size = Size;
        using slot_type = node_head_ptr;

        uint8_t _free_mi, _used_num;
        uint8_t _map[index_size];
        node_head_ptr _slots[size];
    };

    /*
     * Array of pointers on lower nodes
     */
    struct array_of_node_ptrs {
        static constexpr unsigned size = node_index_limit;
        using slot_type = node_head_ptr;

        uint16_t _size;
        node_head_ptr _slots[size];

        void construct() noexcept {
            _size = 0;
            for (unsigned i = 0; i < size; i++) {
                _slots[i] = nullptr;
            }
        }
        void construct(array_of_node_ptrs&& o) noexcept {
            for (unsigned i = 0; i < size; i++) {
                move_slot(std::move(o._slots[i]), &_slots[i]);
            }
            _size = std::exchange(o._size, 0);
        }

        bool has(unsigned i) const noexcept { return _slots[i]; }
        void set(unsigned i) noexcept { _size++; }
        void unset(unsigned i) noexcept { _size--; }
        unsigned count() const noexcept { return _size; }
    };

    /*
     * A header all nodes start with. Type of node (inner/leaf) is
     * evaluated (fingers-crossed) from the depth argument. Kind of
     * the node (its layout) is kept in the _kind member.
     */
    struct node_head {
        node_head_ptr* _backref;
        index_t _prefix;
        node_kind _kind;

        node_head() noexcept : _backref(nullptr), _prefix(0), _kind(node_kind::nil) {}

        node_head(index_t prefix, node_kind kind) noexcept
                : _backref(nullptr), _prefix(prefix), _kind(kind) {}
        node_head(const node_head&) = delete;
        node_head(node_head&& o) noexcept
                : node_head(o._prefix, o._kind) {
            _backref = std::exchange(o._backref, nullptr);
            if (_backref != nullptr) {
                *_backref = this;
            }
        }
        ~node_head() {}

        /*
         * Inner nodes (and the root of the tree) point to the head
         * and call methods on it. When the head finds out who it is
         * it will transform itself into inner/outer node
         */

        template <typename NBT>
        NBT& as_base() noexcept {
            return *boost::intrusive::get_parent_from_member(this, &NBT::_head);
        }

        template <typename NBT>
        const NBT& as_base() const noexcept {
            return *boost::intrusive::get_parent_from_member(this, &NBT::_head);
        }

        template <typename NT>
        NT::node_type& as_base_of() noexcept {
            return as_base<typename NT::node_type>();
        }

        template <typename NT>
        const NT::node_type& as_base_of() const noexcept {
            return as_base<typename NT::node_type>();
        }

        template <typename NT>
        NT& as_node() noexcept {
            return *boost::intrusive::get_parent_from_member(&as_base_of<NT>(), &NT::_base);
        }

        template <typename NT>
        const NT& as_node() const noexcept {
            return *boost::intrusive::get_parent_from_member(&as_base_of<NT>(), &NT::_base);
        }

        // Prefix manipulations
        unsigned prefix_len() const noexcept { return _prefix & prefix_len_mask; }
        void trim_prefix(unsigned v) noexcept { _prefix -= v; }
        void bump_prefix(unsigned v) noexcept { _prefix += v; }

        bool check_prefix(index_t idx, unsigned& depth) const noexcept {
            unsigned real_depth = depth + prefix_len();
            index_t mask = prefix_mask_at(real_depth);
            if ((idx & mask) != (_prefix & mask)) {
                return false;
            }

            depth = real_depth;
            return true;
        }

        /*
         * A bunch of "polymorphic" wrappers.
         */
        const T* get(index_t idx, unsigned depth) const noexcept {
            return depth == leaf_depth ? as_node<leaf_node>().get(idx) : as_node<inner_node>().get(idx, depth);
        }

        lower_bound_res lower_bound(index_t& idx, unsigned depth) const noexcept {
            unsigned real_depth = depth + prefix_len();
            index_t mask = prefix_mask_at(real_depth);
            if ((idx & mask) > (_prefix & mask)) {
                return lower_bound_res(nullptr, nullptr);
            }

            depth = real_depth;
            if (depth == leaf_depth) {
                const T* val = as_base_of<leaf_node>().lower_bound(idx, depth).first;
                return std::pair(val, this);
            } else {
                return as_base_of<inner_node>().lower_bound(idx, depth);
            }
        }

        template <typename Visitor>
        bool visit(Visitor&& v, unsigned depth) const {
            bool ret = true;
            depth += prefix_len();
            if (v(*this, depth, true)) {
                if (depth == leaf_depth) {
                    ret = as_base_of<leaf_node>().visit(v, depth);
                } else {
                    ret = as_base_of<inner_node>().visit(v, depth);
                }
                v(*this, depth, false);
            }
            return ret;
        }

        allocate_res alloc(index_t idx, unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().alloc(idx, depth);
            } else {
                return as_base_of<inner_node>().alloc(idx, depth);
            }
        }

        erase_result erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().erase(idx, depth, erm);
            } else {
                return as_base_of<inner_node>().erase(idx, depth, erm);
            }
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().weed(filter, depth);
            } else {
                return as_base_of<inner_node>().weed(filter, depth);
            }
        }

        node_head* grow(unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().template grow<leaf_node>();
            } else {
                return as_base_of<inner_node>().template grow<inner_node>();
            }
        }

        node_head* shrink(unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().template shrink<leaf_node>();
            } else {
                return as_base_of<inner_node>().template shrink<inner_node>();
            }
        }

        void free(unsigned depth) noexcept {
            if (depth == leaf_depth) {
                leaf_node::free(as_node<leaf_node>());
            } else {
                inner_node::free(as_node<inner_node>());
            }
        }

        /*
         * A leaf-node specific helper for iterator
         */
        const T* lower_bound(index_t& idx) const noexcept {
            return as_base_of<leaf_node>().lower_bound(idx, leaf_depth).first;
        }

        /*
         * And two inner-node specific calls for nodes
         * squashing/expanding
         */

        void set_lower(node_index_t idx, node_head* n) noexcept {
            as_node<inner_node>().set_lower(idx, n);
        }

        const node_head_ptr* pop_lower(node_index_t& idx) const noexcept {
            return as_node<inner_node>().pop_lower(idx);
        }
    };

    /*
     * Pointer to node head. Inner nodes keep these, tree root pointer
     * is the one as well.
     */
    class node_head_ptr {
        node_head* _v;

    public:
        node_head_ptr(node_head* v) noexcept : _v(v) {}
        node_head_ptr(const node_head_ptr&) = delete;
        node_head_ptr(node_head_ptr&& o) noexcept : _v(std::exchange(o._v, nullptr)) {
            if (_v != nullptr) {
                _v->_backref = this;
            }
        }

        node_head& operator*() const noexcept { return *_v; }
        node_head* operator->() const noexcept { return _v; }
        node_head* raw() const noexcept { return _v; }

        operator bool() const noexcept { return _v != nullptr; }
        bool is(const node_head& n) const noexcept { return _v == &n; }

        node_head_ptr& operator=(node_head* v) noexcept {
            _v = v;
            if (_v != nullptr) {
                _v->_backref = this;
            }
            return *this;
        }
    };

    node_head_ptr _root;
    static node_head nil_root;

    /*
     * This helper wraps several layouts into one and preceeds them with
     * the header. It does nothing but provides a polymorphic calls to the
     * lower/inner layouts depending on the head.kind value.
     */
    template <typename Slot, typename... Layouts>
    struct node_base {
        node_head _head; // _head has 3 bytes gap afterwards
        variadic_union<true, Layouts...> _layouts;

        template <bool Sentinel>
        static size_t node_size(node_kind kind) noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        static size_t node_size(node_kind kind) noexcept {
            return kind == Tx::kind ? sizeof(node_head) + sizeof(Tx) : node_size<Sentinel, Ts...>(kind);
        }

        static size_t node_size(node_kind kind) noexcept {
            return node_size<true, Layouts...>(kind);
        }

        /*
         * Constructing actual node layouts. Using the .construct() method
         * instead of real constructors saves us from using more nested
         * unions to stop compiler from default-initializating slots
         */
        template <bool Sentinel>
        void construct(variadic_union<Sentinel>& cur) noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        void construct(variadic_union<Sentinel, Tx, Ts...>& cur) noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.construct();
            }

            construct<Sentinel, Ts...>(cur._other);
        }

        node_base(index_t prefix, node_kind kind) noexcept : _head(prefix, kind) {
            construct<true, Layouts...>(_layouts);
        }

        node_base(const node_base&) = delete;

        template <bool Sentinel>
        void construct(variadic_union<Sentinel>& cur, variadic_union<Sentinel>&& o) noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        void construct(variadic_union<Sentinel, Tx, Ts...>& cur, variadic_union<Sentinel, Tx, Ts...>&& o) noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.construct(std::move(o._this));
            }

            construct<Sentinel, Ts...>(cur._other, std::move(o._other));
        }

        node_base(node_base&& o) noexcept : _head(std::move(o._head)) {
            construct<true, Layouts...>(_layouts, std::move(o._layouts));
        }

        ~node_base() { }

        // Finds an index within a node
        template <bool Sentinel>
        const Slot* find(const variadic_union<Sentinel>& cur, node_index_t idx) const noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        const Slot* find(const variadic_union<Sentinel, Tx, Ts...>& cur, node_index_t idx) const noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.find(idx);
            }

            return find<Sentinel, Ts...>(cur._other, idx);
        }

        const Slot* find(node_index_t idx) const noexcept {
            if (_head._kind == node_kind::nil) {
                return (const Slot *)nullptr;
            }

            return find<true, Layouts...>(_layouts, idx);
        }

        // Finds a lowed-bound element for an index
        template <bool Sentinel>
        lower_bound_res lower_bound(const variadic_union<Sentinel>& cur, index_t& idx, unsigned depth) const noexcept {
            __builtin_unreachable();
        }

        template <bool Sentinel, typename Tx, typename... Ts>
        lower_bound_res lower_bound(const variadic_union<Sentinel, Tx, Ts...>& cur, index_t& idx, unsigned depth) const noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.lower_bound(idx, _head._prefix, depth);
            }

            return lower_bound<Sentinel, Ts...>(cur._other, idx, depth);
        }

        lower_bound_res lower_bound(index_t& idx, unsigned depth) const noexcept {
            if (_head._kind == node_kind::nil) {
                return lower_bound_res(nullptr, nullptr);
            }

            return lower_bound<true, Layouts...>(_layouts, idx, depth);
        }

        // Erase an index from node
        template <bool Sentinel>
        erase_result erase(variadic_union<Sentinel>& cur, index_t idx, unsigned depth, erase_mode erm) noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        erase_result erase(variadic_union<Sentinel, Tx, Ts...>& cur, index_t idx, unsigned depth, erase_mode erm) noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.erase(idx, depth, erm);
            }

            return erase<Sentinel, Ts...>(cur._other, idx, depth, erm);
        }

        erase_result erase(index_t idx, unsigned depth, erase_mode erm) noexcept {
            return erase<true, Layouts...>(_layouts, idx, depth, erm);
        }

        // Weed node with filter
        template <bool Sentinel, typename Fn>
        erase_result weed(variadic_union<Sentinel>& cur, Fn&& filter, unsigned depth) { __builtin_unreachable(); }

        template <bool Sentinel, typename Fn, typename Tx, typename... Ts>
        erase_result weed(variadic_union<Sentinel, Tx, Ts...>& cur, Fn&& filter, unsigned depth) {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.weed(filter, _head._prefix, depth);
            }

            return weed<Sentinel, Fn, Ts...>(cur._other, filter, depth);
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, unsigned depth) {
            return weed<true, Fn, Layouts...>(_layouts, filter, depth);
        }

        /*
         * Find the index on a node or allocates a new one. If
         * no space left -- returns nullptr.
         */

        template <bool Sentinel>
        allocate_res alloc(variadic_union<Sentinel>& cur, index_t idx, unsigned depth) { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        allocate_res alloc(variadic_union<Sentinel, Tx, Ts...>& cur, index_t idx, unsigned depth) {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.alloc(idx, depth);
            }

            return alloc<Sentinel, Ts...>(cur._other, idx, depth);
        }

        allocate_res alloc(index_t idx, unsigned depth) {
            return alloc<true, Layouts...>(_layouts, idx, depth);
        }

        /*
         * Finds and occupies a free slot on a node. The slot MUST exist
         * and MUST be initialized right after reservation
         */

        template <bool Sentinel>
        Slot* reserve(variadic_union<Sentinel>& cur, node_index_t idx) noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        Slot* reserve(variadic_union<Sentinel, Tx, Ts...>& cur, node_index_t idx) noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.reserve(idx);
            }

            return reserve<Sentinel, Ts...>(cur._other, idx);
        }

        Slot* reserve(node_index_t idx) noexcept {
            return reserve<true, Layouts...>(_layouts, idx);
        }

        /*
         * Locates the first element in the node. Used by squashing
         * code when it finds out that there's only one index left
         * on the node.
         */

        template <bool Sentinel>
        const Slot* pop(const variadic_union<Sentinel>& cur, node_index_t& idx) const noexcept { __builtin_unreachable(); }

        template <bool Sentinel, typename Tx, typename... Ts>
        const Slot* pop(const variadic_union<Sentinel, Tx, Ts...>& cur, node_index_t& idx) const noexcept {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.pop(idx);
            }

            return pop<Sentinel, Ts...>(cur._other, idx);
        }

        const Slot* pop(node_index_t& idx) const noexcept {
            return pop<true, Layouts...>(_layouts, idx);
        }

        // Visiting
        template <bool Sentinel, typename Visitor>
        bool visit(const variadic_union<Sentinel>& cur, Visitor&& v, unsigned depth) const { __builtin_unreachable(); }

        template <bool Sentinel, typename Visitor, typename Tx, typename... Ts>
        bool visit(const variadic_union<Sentinel, Tx, Ts...>& cur, Visitor&& v, unsigned depth) const {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                return cur._this.visit(v, _head._prefix, depth);
            }

            return visit<Sentinel, Visitor, Ts...>(cur._other, v, depth);
        }

        template <typename Visitor>
        bool visit(Visitor&& v, unsigned depth) const {
            return visit<true, Visitor, Layouts...>(_layouts, v, depth);
        }

        /*
         * Growing and shrinking. Layouts grow into the next one
         * in Layouts list, shrink into previous. This is not very
         * flexible, but lets keeping layouts independent.
         */
        template <bool Sentinel, typename NT, typename Tx>
        node_head* grow(variadic_union<Sentinel, Tx>& cur) {
            std::abort(); // cannot grow large node
        }

        template <bool Sentinel, typename NT, typename Tx, typename Tn, typename... Ts>
        node_head* grow(variadic_union<Sentinel, Tx, Tn, Ts...>& cur) {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._this.kind);
                NT* nn = NT::template allocate<Tn>(_head._prefix);
                cur._this.grow(nn->_base);
                return &nn->_base._head;
            }

            return grow<Sentinel, NT, Tn, Ts...>(cur._other);
        }

        template <typename NT>
        node_head* grow() {
            return grow<true, NT, Layouts...>(_layouts);
        }

        template <bool Sentinel, typename NT, typename Tx>
        node_head* shrink(variadic_union<Sentinel, Tx>& cur) {
            std::abort(); // cannot shrink into large layout
        }

        template <bool Sentinel, typename NT, typename Tp, typename Tx, typename... Ts>
        node_head* shrink(variadic_union<Sentinel, Tp, Tx, Ts...>& cur) {
            if (_head._kind == Tx::kind) {
                static_assert(Tx::kind == cur._other._this.kind);
                NT* nn = NT::template allocate<Tp>(_head._prefix);
                cur._other._this.shrink(nn->_base);
                return &nn->_base._head;
            }

            return shrink<Sentinel, NT, Tx, Ts...>(cur._other);
        }

        template <typename NT>
        node_head* shrink() {
            return shrink<true, NT, Layouts...>(_layouts);
        }
    };

    template <typename SlotType>
    static void move_slot(SlotType&& slot, SlotType* to) noexcept {
        new (to) SlotType(std::move(slot));
        slot.~SlotType();
    }

    template <typename LT, typename SlotType>
    static void move_slot(SlotType&& slot, LT& into, unsigned idx) noexcept {
        SlotType* el = into.reserve(idx);
        move_slot<SlotType>(std::move(slot), el);
    }

    /*
     * Expand a node that failed prefix check.
     * Turns a node with non-zero prefix on which parent tries to allocate
     * an index beyond its limits. For this:
     * - the inner node is allocated on the level, that's enough to fit
     *   both -- current node and the desired index
     * - the given node is placed into this new inner one at the index it's
     *   expected to be found there (the prefix value)
     * - the allocation continues on this new inner (with fixed depth)
     */
    static node_head* expand(node_head* n, index_t idx, unsigned& depth) {
        index_t n_prefix = n->_prefix;

        /*
         * The plen is the level at which current node and desired
         * index still coinside
         */
        unsigned plen = common_prefix_len(idx, n_prefix);
        assert(plen >= depth);
        plen -= depth;
        depth += plen;
        assert(n->prefix_len() > plen);

        node_head* ni = inner_node::allocate_initial(idx, plen);
        // Trim all common nodes + ni one from n
        n->trim_prefix(plen + 1);
        ni->set_lower(node_index(n_prefix, depth), n);

        return ni;
    }

    /*
     * Pop one the single lower node and prepare it to replace the
     * current one. This preparation is purely increasing its prefix
     * len, as the prefix value itself is already correct
     */
    static node_head* squash(node_head* n, unsigned depth) noexcept {
        node_index_t idx;
        const node_head_ptr* np = n->pop_lower(idx);
        assert(np != nullptr);
        node_head* kid = np->raw();
        // Kid has n and it's prefix squashed
        kid->bump_prefix(n->prefix_len() + 1);
        return kid;
    }

    static bool maybe_drop_from(node_head_ptr* np, erase_result res, unsigned depth) noexcept {
        node_head* n = np->raw();

        switch (res) {
        case erase_result::empty:
            n->free(depth);
            *np = nullptr;
            return true;

        case erase_result::squash:
            if (depth != leaf_depth) {
                *np = squash(n, depth);
                n->free(depth);
            }
            break;
        case erase_result::shrink:
            try {
                *np = n->shrink(depth);
                n->free(depth);
            } catch(...) {
                /*
                 * The node tried to shrink but failed to
                 * allocate memory for the new layout. This
                 * is not that bad, it can survive in current
                 * layout and be shrunk (or squashed or even
                 * dropped) later.
                 */
            }
            break;
        case erase_result::nothing: ; // make compiler happy
        }

        return false;
    }

    class leaf_node {
        template <typename A, typename B> friend class printer;
        friend class ::size_calculator;
        template <typename A, typename... L> friend class node_base;
        friend class node_head;

        using tiny_node = minimal_table_layout<table_of_values<4, uint8_t>, node_kind::tiny>;
        using small_node = table_layout<table_of_values<16, uint16_t>, node_kind::small, tiny_node::size>;
        using medium_node = map_layout<map_of_values<48>, node_kind::medium, small_node::size>;
        using large_node = array_layout<array_of_values, node_kind::large, medium_node::size>;

    public:
        using node_type = node_base<T, tiny_node, small_node, medium_node, large_node>;

        static node_head* allocate_initial(index_t prefix, unsigned len) {
            return &allocate<tiny_node>(node_prefix(prefix, len))->_base._head;
        }

        leaf_node(leaf_node&& other) noexcept : _base(std::move(other._base)) {}
        ~leaf_node() { }

        friend size_t size_for_allocation_strategy(const leaf_node& n) noexcept {
            return node_type::node_size(n._base._head._kind);
        }

    private:
        node_type _base;

        leaf_node(index_t prefix, node_kind kind) noexcept : _base(prefix, kind) { }
        leaf_node(const leaf_node&) = delete;

        template <typename KT>
        static leaf_node* allocate(index_t prefix) {
            void* mem = current_allocator().alloc(
                    &get_standard_migrator<leaf_node>(),
                    node_type::node_size(KT::kind), alignof(node_head));
            return new (mem) leaf_node(prefix, KT::kind);
        }

        static void free(leaf_node& node) noexcept {
            node.~leaf_node();
            current_allocator().free(&node, node_type::node_size(node._base._head._kind));
        }

        const T* get(index_t idx) const noexcept {
            return _base.find(node_index(idx, leaf_depth));
        }
    };

    class inner_node {
        template <typename A, typename B> friend class printer;
        friend class ::size_calculator;
        template <typename A, typename... L> friend class node_base;
        friend class node_head;

        using tiny_node = minimal_table_layout<table_of_node_ptrs<4>, node_kind::tiny>;
        using small_node = table_layout<table_of_node_ptrs<16>, node_kind::small, tiny_node::size>;
        using medium_node = map_layout<map_of_node_ptrs<48>, node_kind::medium, small_node::size>;
        using large_node = array_layout<array_of_node_ptrs, node_kind::large, medium_node::size>;

    public:
        using node_type = node_base<node_head_ptr, tiny_node, small_node, medium_node, large_node>;

        static node_head* allocate_initial(index_t prefix, unsigned len) {
            return &allocate<tiny_node>(node_prefix(prefix, len))->_base._head;
        }

        inner_node(inner_node&& other) noexcept : _base(std::move(other._base)) {}
        ~inner_node() {}

        friend size_t size_for_allocation_strategy(const inner_node& n) noexcept {
            return node_type::node_size(n._base._head._kind);
        }

    private:
        node_type _base;

        inner_node(index_t prefix, node_kind kind) noexcept : _base(prefix, kind) {}
        inner_node(const inner_node&) = delete;

        template <typename KT>
        static inner_node* allocate(index_t prefix) {
            void* mem = current_allocator().alloc(
                &get_standard_migrator<inner_node>(),
                node_type::node_size(KT::kind), alignof(node_head));
            return new (mem) inner_node(prefix, KT::kind);
        }

        static void free(inner_node& node) noexcept {
            node.~inner_node();
            current_allocator().free(&node, node_type::node_size(node._base._head._kind));
        }

        const T* get(index_t idx, unsigned depth) const noexcept {
            const node_head_ptr* np = _base.find(node_index(idx, depth));
            if (np == nullptr) {
                return nullptr;
            }

            return get_at(np, idx, depth + 1);
        }

        const node_head_ptr* pop_lower(node_index_t& idx) const noexcept {
            return _base.pop(idx);
        }

        void set_lower(node_index_t idx, node_head* n) noexcept {
            node_head_ptr* np = _base.reserve(idx);
            *np = n;
        }
    };

    static const T* get_at(const node_head_ptr* np, index_t idx, unsigned depth = 0) noexcept {
        const node_head* n = np->raw();

        if (!n->check_prefix(idx, depth)) {
            return nullptr;
        }

        return n->get(idx, depth);
    }

    static allocate_res allocate_on(T* val, index_t idx, unsigned depth, bool allocated) noexcept {
        return allocate_res(val, allocated);
    }

    static allocate_res allocate_on(node_head_ptr* np, index_t idx, unsigned depth = 0, bool _ = false) {
        node_head* n = np->raw();

        if (!n->check_prefix(idx, depth)) {
            *np = n = expand(n, idx, depth);
        }

        allocate_res ret = n->alloc(idx, depth);
        if (ret.first == nullptr) {
            /*
             * The nullptr ret means the n has run out of
             * free slots. Grow one into bigger layout and
             * try again
             */
            node_head* nn = n->grow(depth);
            n->free(depth);
            *np = nn;
            ret = nn->alloc(idx, depth);
            assert(ret.first != nullptr);
        }
        return ret;
    }

    // Populating value slot happens in tree::emplace
    static void populate_slot(T* val, index_t idx, unsigned depth) noexcept { }

    static void populate_slot(node_head_ptr* np, index_t idx, unsigned depth) {
        /*
         * Allocate leaf immediatelly with the prefix
         * len big enough to cover all skipped node
         * up to the current depth
         */
        assert(leaf_depth >= depth + 1);
        *np = leaf_node::allocate_initial(idx, leaf_depth - (depth + 1));
    }

    template <typename Visitor>
    static bool visit_slot(Visitor&& v, index_t pfx, node_index_t ni, const T* val, unsigned depth) {
        return v(tree_index(pfx, ni), *val);
    }

    template <typename Visitor>
    static bool visit_slot(Visitor&& v, index_t, node_index_t, const node_head_ptr* ptr, unsigned depth) {
        return (*ptr)->visit(v, depth + 1);
    }

    static lower_bound_res lower_bound_at(index_t pfx, node_index_t ni, const T* val, unsigned depth, index_t& idx) noexcept {
        idx = tree_index(pfx, ni);
        return lower_bound_res(val, nullptr);
    }

    static lower_bound_res lower_bound_at(index_t, node_index_t, const node_head_ptr* ptr, unsigned depth, index_t& idx) noexcept {
        return (*ptr)->lower_bound(idx, depth + 1);
    }

    template <typename Fn>
    static bool weed_from_slot(index_t prefix, node_index_t ni, T* val, Fn&& filter, unsigned depth) {
        if (!filter(tree_index(prefix, ni), *val)) {
            return false;
        }

        val->~T();
        return true;
    }

    template <typename Fn>
    static bool weed_from_slot(index_t, node_index_t, node_head_ptr* np, Fn&& filter, unsigned depth) {
        node_head* n = np->raw();
        depth += n->prefix_len();

        erase_result er = n->weed(filter, depth);

        // FIXME -- after weed the node might want to shrink into
        // even smaller, than just previous, layout
        return maybe_drop_from(np, er, depth);
    }

    static bool erase_from_slot(T* val, index_t idx, unsigned depth, erase_mode erm) noexcept {
        if (erm == erase_mode::real) {
            val->~T();
        }

        return true;
    }

    static bool erase_from_slot(node_head_ptr* np, index_t idx, unsigned depth, erase_mode erm) noexcept {
        node_head* n = np->raw();
        assert(n->check_prefix(idx, depth));

        erase_result er = n->erase(idx, depth, erm);
        if (erm == erase_mode::cleanup) {
            return false;
        }

        return maybe_drop_from(np, er, depth);
    }

private:
    template <typename Visitor>
    void visit(Visitor&& v) const {
        if (!_root.is(nil_root)) {
            _root->visit(std::move(v), 0);
        }
    }

    template <typename Visitor>
    void visit(Visitor&& v) {
        struct adaptor {
            Visitor&& v;
            bool sorted;

            bool operator()(index_t idx, const T& val) {
                return v(idx, const_cast<T&>(val));
            }

            bool operator()(const node_head& n, unsigned depth, bool enter) {
                return v(const_cast<node_head&>(n), depth, enter);
            }
        };

        const_cast<const tree*>(this)->visit(adaptor{std::move(v), v.sorted});
    }

public:
    tree() noexcept : _root(&nil_root) {}
    ~tree() {
        clear();
    }

    tree(const tree&) = delete;
    tree(tree&& o) noexcept : _root(std::exchange(o._root, &nil_root)) {}

    const T* get(index_t idx) const noexcept {
        return get_at(&_root, idx);
    }

    T* get(index_t idx) noexcept {
        return const_cast<T*>(get_at(&_root, idx));
    }

    const T* lower_bound(index_t idx) const noexcept {
        return _root->lower_bound(idx, 0).first;
    }

    T* lower_bound(index_t idx) noexcept {
        return const_cast<T*>(
            const_cast<const tree*>(this)->lower_bound(idx)
        );
    }

    template <typename... Args>
    void emplace(index_t idx, Args&&... args) {
        if (_root.is(nil_root)) {
            _root = leaf_node::allocate_initial(idx, 3);
        }

        allocate_res v = allocate_on(&_root, idx);
        if (!v.second) {
            v.first->~T();
        }

        try {
            new (v.first) T(std::forward<Args>(args)...);
        } catch (...) {
            erase_from_slot(&_root, idx, 0, erase_mode::cleanup);
            throw;
        }
    }

    void erase(index_t idx) noexcept {
        if (!_root.is(nil_root)) {
            erase_from_slot(&_root, idx, 0, erase_mode::real);
            if (!_root) {
                _root = &nil_root;
            }
        }
    }

    void clear() noexcept {
        struct clearing_visitor {
            bool sorted = false;

            bool operator()(index_t idx, T& val) noexcept {
                val.~T();
                return true;
            }
            bool operator()(node_head& n, unsigned depth, bool enter) noexcept {
                if (!enter) {
                    n.free(depth);
                }
                return true;
            }
        };

        visit(clearing_visitor{});
        _root = &nil_root;
    }

    template <typename Fn>
    requires requires (Fn fn, index_t idx, T val) { { fn(idx, val) } -> std::same_as<bool>; }
    void weed(Fn&& filter) {
        if (!_root.is(nil_root)) {
            weed_from_slot(0, 0, &_root, filter, 0);
            if (!_root) {
                _root = &nil_root;
            }
        }
    }

private:
    template <typename Fn, bool Const>
    struct walking_visitor {
            Fn&& fn;
            bool sorted;

            using value_t = std::conditional_t<Const, const T, T>;
            using node_t = std::conditional_t<Const, const node_head, node_head>;

            bool operator()(index_t idx, value_t& val) {
                return fn(idx, val);
            }
            bool operator()(node_t& n, unsigned depth, bool enter) noexcept {
                return true;
            }
    };

public:
    template <typename Fn>
    requires requires (Fn fn, index_t idx, const T val) { { fn(idx, val) } -> std::same_as<bool>; }
    void walk(Fn&& fn, bool sorted = true) const {
        visit(walking_visitor<Fn, true>{std::move(fn), sorted});
    }

    template <typename Fn>
    requires requires (Fn fn, index_t idx, T val) { { fn(idx, val) } -> std::same_as<bool>; }
    void walk(Fn&& fn, bool sorted = true) {
        visit(walking_visitor<Fn, false>{std::move(fn), sorted});
    }

    template <bool Const>
    class iterator_base {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::conditional_t<Const, const T, T>;
        using difference_type = ssize_t;
        using pointer = value_type*;
        using reference = value_type&;

    private:
        index_t _idx = 0;
        pointer _value = nullptr;
        const tree* _tree = nullptr;
        const node_head* _leaf = nullptr;

    public:
        index_t index() const noexcept { return _idx; }

        iterator_base() noexcept = default;
        iterator_base(const tree* t) noexcept : _tree(t) {
            lower_bound_res res = _tree->_root->lower_bound(_idx, 0);
            _value = const_cast<pointer>(res.first);
            _leaf = res.second;
        }

        iterator_base& operator++() noexcept {
            if (_value == nullptr) {
                _value = nullptr;
                return *this;
            }

            _idx++;
            if (node_index(_idx, leaf_depth) != 0) {
                /*
                 * Short-cut. If we're still inside the leaf,
                 * then it's worth trying to shift forward on
                 * it without messing with upper levels
                 */
                _value = const_cast<pointer>(_leaf->lower_bound(_idx));
                if (_value != nullptr) {
                    return *this;
                }

                /*
                 * No luck. Go ahead and scan the tree from top
                 * again. It's only leaf_depth levels though. Also
                 * not to make the call below visit this leaf one
                 * more time, bump up the index to move out of the
                 * current leaf and keep the leaf's part zero.
                 */

                 _idx += node_index_limit;
                 _idx &= ~((index_t)0xff);
            }

            lower_bound_res res = _tree->_root->lower_bound(_idx, 0);
            _value = const_cast<pointer>(res.first);
            _leaf = res.second;

            return *this;
        }

        iterator_base operator++(int) noexcept {
            iterator_base cur = *this;
            operator++();
            return cur;
        }

        pointer operator->() const noexcept { return _value; }
        reference operator*() const noexcept { return *_value; }

        bool operator==(const iterator_base& o) const noexcept { return _value == o._value; }
        bool operator!=(const iterator_base& o) const noexcept { return !(*this == o); }
    };

    using iterator = iterator_base<false>;
    using const_iterator = iterator_base<true>;

    iterator begin() noexcept { return iterator(this); }
    iterator end() noexcept { return iterator(); }
    const_iterator cbegin() const noexcept { return const_iterator(this); }
    const_iterator cend() const noexcept { return const_iterator(); }
    const_iterator begin() const noexcept { return cbegin(); }
    const_iterator end() const noexcept { return cend(); }

    bool empty() const noexcept { return _root.is(nil_root); }

    template <typename Fn>
    requires requires (Fn fn, index_t idx, const T val) { { fn(idx, val) } noexcept -> std::same_as<size_t>; }
    size_t memory_usage(Fn&& entry_mem_usage) const noexcept {
        struct counting_visitor {
                Fn&& entry_mem_usage;
                bool sorted = false;
                size_t mem = 0;

                bool operator()(index_t idx, const T& val) {
                    mem += entry_mem_usage(idx, val);
                    return true;
                }
                bool operator()(const node_head& n, unsigned depth, bool enter) noexcept {
                    if (enter) {
                        mem += depth == leaf_depth ?
                                leaf_node::node_type::node_size(n._kind) :
                                inner_node::node_type::node_size(n._kind);
                    }
                    return true;
                }
        };

        counting_visitor v{std::move(entry_mem_usage)};
        visit(v);

        return v.mem;
    }
};

} // namespace
