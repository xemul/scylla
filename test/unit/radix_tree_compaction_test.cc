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

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <map>
#include <vector>
#include <random>
#include <string>
#include <iostream>
#include <fmt/core.h>
#include "utils/logalloc.hh"

#include "utils/compact-radix-tree.hh"
#include "radix_tree_printer.hh"

using namespace compact_radix_tree;
using namespace seastar;

class test_data {
    unsigned long *_data;
    unsigned long _val;
public:
    test_data(unsigned long val) : _data(new unsigned long(val)), _val(val) {}
    test_data(const test_data&) = delete;
    test_data(test_data&& o) noexcept : _data(std::exchange(o._data, nullptr)), _val(o._val) {}
    ~test_data() {
        if (_data != nullptr) {
            delete _data;
        }
    }

    unsigned long value() const {
        return _data == nullptr ? _val + 0x80000000 : *_data;
    }
};

std::ostream& operator<<(std::ostream& out, const test_data& d) {
    out << d.value();
    return out;
}

using test_tree = tree<test_data>;

class reference {
    reference* _ref = nullptr;
public:
    reference() = default;
    reference(const reference& other) = delete;

    reference(reference&& other) noexcept : _ref(other._ref) {
        if (_ref != nullptr) {
            _ref->_ref = this;
        }
        other._ref = nullptr;
    }

    ~reference() {
        if (_ref != nullptr) {
            _ref->_ref = nullptr;
        }
    }

    void link(reference& other) {
        assert(_ref == nullptr);
        _ref = &other;
        other._ref = this;
    }

    reference* get() {
        assert(_ref != nullptr);
        return _ref;
    }
};

class tree_pointer {
    reference _ref;

    class tree_wrapper {
        friend class tree_pointer;
        test_tree _tree;
        reference _ref;
    public:
        tree_wrapper() : _tree() {}
    };

    tree_wrapper* get_wrapper() {
        return boost::intrusive::get_parent_from_member(_ref.get(), &tree_wrapper::_ref);
    }

public:

    tree_pointer(const tree_pointer& other) = delete;
    tree_pointer(tree_pointer&& other) = delete;

    tree_pointer() {
        tree_wrapper *t = current_allocator().construct<tree_wrapper>();
        _ref.link(t->_ref);
    }

    test_tree* operator->() {
        tree_wrapper *tw = get_wrapper();
        return &tw->_tree;
    }

    test_tree& operator*() {
        tree_wrapper *tw = get_wrapper();
        return tw->_tree;
    }

    ~tree_pointer() {
        tree_wrapper *tw = get_wrapper();
        current_allocator().destroy(tw);
    }
};

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(132564), "number of indices to fill the tree with")
        ("sparse", bpo::value<int>()->default_value(3), "sparse factor (max index value will be count * sparse)")
        ("iters", bpo::value<int>()->default_value(13), "number of iterations")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto sparse = std::max(app.configuration()["sparse"].as<int>(), 1);
        auto iter = app.configuration()["iters"].as<int>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, sparse, iter, verb] {
            std::vector<int> keys;
            for (int i = 0; i < count * sparse; i++) {
                keys.push_back(i + 1);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Compacting {:d} (sparse {:d}) k:v pairs {:d} times\n", count, sparse, iter);

            logalloc::region mem;

            with_allocator(mem.allocator(), [&] {
                tree_pointer t;

                for (auto rep = 0; rep < iter; rep++) {
                    fmt::print("Populating tree\n");
                    {
                        logalloc::reclaim_lock rl(mem);

                        std::shuffle(keys.begin(), keys.end(), g);
                        int nr = 0;
                        for (int i = 0; i < count * sparse; i++) {
                            if (t->get(keys[i]) == nullptr) {
                                t->emplace(keys[i], keys[i]);
                                nr++;
                            }
                        }

                        if (verb) {
                            fmt::print("After add\n");
                            compact_radix_tree::printer<test_data, unsigned>::show(*t);
                        }

                        std::shuffle(keys.begin(), keys.end(), g);
                        for (int i = 0; i < count * (sparse - 1); i++) {
                            t->erase(keys[i]);
                        }

                        if (verb) {
                            fmt::print("After erase\n");
                            compact_radix_tree::printer<test_data, unsigned>::show(*t);
                        }
                    }

                    fmt::print("Compacting\n");
                    mem.full_compaction();

                    if (verb) {
                        fmt::print("After fill + compact\n");
                        compact_radix_tree::printer<test_data, unsigned>::show(*t);
                    }

                    fmt::print("Validating\n");
                    int nr = 0;
                    auto ti = t->begin();
                    while (ti != t->end()) {
                        assert(ti->value() == ti.index());
                        nr++;
                        ti++;
                    }
                    fmt::print("`-> {} entries OK\n", nr);
                }
            });
        });
    });
}

namespace compact_radix_tree {
template<>
test_tree::node_head test_tree::nil_root = test_tree::node_head();
}
