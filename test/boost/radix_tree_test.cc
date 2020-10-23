
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

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <fmt/core.h>

#include "utils/compact-radix-tree.hh"

using namespace compact_radix_tree;
using namespace seastar;

class test_data {
    unsigned long _val;
    unsigned long *_pval;
public:
    test_data(unsigned long val) : _val(val), _pval(new unsigned long(val)) {}
    test_data(const test_data&) = delete;
    test_data(test_data&& o) noexcept : _val(o._val), _pval(std::exchange(o._pval, nullptr)) {}
    ~test_data() {
        if (_pval != nullptr) {
            delete _pval;
        }
    }

    unsigned long value() const {
        return _pval != nullptr ? *_pval : _val + 1000000;
    }
};

std::ostream& operator<<(std::ostream& out, const test_data& d) {
    out << d.value();
    return out;
}

using test_tree = tree<test_data>;

SEASTAR_TEST_CASE(test_exception_safety_of_emplace) {
    return seastar::async([] {
        test_tree tree;
        memory::with_allocation_failures([&] {
            for (int i = 0; i < 1024; i++) {
                if (tree.get(i) == nullptr) {
                    tree.emplace(i, i);
                }
            }

            fmt::print("checking\n");
            auto it = tree.begin();
            while (it != tree.end()) {
                assert(it.index() == it->value());
                it++;
            }

            fmt::print("clear\n");
            for (int i = 0; i < 1024; i += 2) {
                tree.erase(i);
            }

            tree.clear();
        });
        assert(tree.empty());
    });
}

namespace compact_radix_tree {
template<>
test_tree::node_head test_tree::nil_root = test_tree::node_head();
}
