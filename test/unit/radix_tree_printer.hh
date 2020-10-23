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

#include <fmt/core.h>

namespace compact_radix_tree {

template <typename T, typename Idx>
class printer {
	using tree_t = tree<T, Idx>;
	using node_head_t = typename tree_t::node_head;
	using leaf_node_t = typename tree_t::leaf_node;
	using inner_node_t = typename tree_t::inner_node;
	using node_kind = typename tree_t::node_kind;

	static std::string node_id(const node_head_t& n) {
		return fmt::format("{:03x}", reinterpret_cast<uintptr_t>(&n) & 0xfff);
	}

	static std::string format(const T& val) noexcept { return fmt::format("{}", val); }
	static std::string format(const typename tree_t::node_head_ptr& p) noexcept { return node_id(*(p.raw())); }

	template <typename Tbl>
	static void print_table(const node_head_t& head, const Tbl& table, unsigned depth, std::string id) {
		fmt::print("{:<{}}{}.table{}.{} @{}/{}.{}:", " ", int(depth * 2), id, Tbl::size, table._data.count(), depth, head._prefix & 0xffffff00, head._prefix & 0xff);
		for (unsigned i = 0; i < Tbl::size; i++) {
			if (table._data.has(i)) {
				fmt::print(" [{}] {}:{}", i, table._data._idx[i], format(table._data._slots[i]));
			}
		}
		fmt::print("\n");
	}

	template <typename Map>
	static void print_map(const node_head_t& head, const Map& map, unsigned depth, std::string id) {
		fmt::print("{:<{}}{}.map @{}/{}.{}:", " ", int(depth * 2), id, depth, head._prefix & 0xffffff00, head._prefix & 0xff);
		for (unsigned i = 0; i < tree_t::node_index_limit; i++) {
			uint8_t mi = map._data._map[i];
			if (mi != Map::unused) {
				fmt::print(" [{}] {}:{}", i, mi, format(map._data._slots[mi]));
			}
		}
		fmt::print("\n");
	}


	template <typename Arr>
	static void print_array(const node_head_t& head, const Arr& array, unsigned depth, std::string id) {
		fmt::print("{:<{}}{}.array.{} @{}/{}.{}:", " ", int(depth * 2), id, array._data.count(), depth, head._prefix & 0xffffff00, head._prefix & 0xff);
		for (unsigned i = 0; i < tree_t::node_index_limit; i++) {
			if (array._data.has(i)) {
				fmt::print(" [{}] {}", i, format(array._data._slots[i]));
			}
		}
		fmt::print("\n");
	}

	template <typename NT>
	static void print(const NT& n, unsigned depth) {
		switch (n._base._head._kind) {
		case node_kind::tiny: return print_table(n._base._head, n._base._layouts._this, depth, node_id(n._base._head));
		case node_kind::small: return print_table(n._base._head, n._base._layouts._other._this, depth, node_id(n._base._head));
		case node_kind::medium: return print_map(n._base._head, n._base._layouts._other._other._this, depth, node_id(n._base._head));
		case node_kind::large: return print_array(n._base._head, n._base._layouts._other._other._other._this, depth, node_id(n._base._head));
		case node_kind::nil: break;
		}
		__builtin_unreachable();
	}

	static void print(const node_head_t& n, unsigned depth) {
		if (depth == tree_t::leaf_depth) {
			print(n.template as_node<leaf_node_t>(), depth);
		} else {
			print(n.template as_node<inner_node_t>(), depth);
		}
	}

public:
	static void show(const tree_t& t) {
		struct printing_visitor {
			bool sorted = false;

			bool operator()(Idx idx, const T& val) {
				std::abort();
			}
			bool operator()(const node_head_t& n, unsigned depth, bool enter) {
				if (enter) {
					print(n, depth);
				}
				return depth != tree_t::leaf_depth;
			}
		};

		fmt::print("tree:\n");
		t.visit(printing_visitor{});
		fmt::print("---\n");
	}
};

} // namespace
