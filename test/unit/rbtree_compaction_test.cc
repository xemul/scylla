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

#include "tree_test_key.hh"
#include "utils/intrusive-rbtree.hh"
#include "rbtree_validation.hh"

using namespace intrusive_rb;
using namespace seastar;

class test_key : public tree_test_key_base {
public:
    member_hook hook;

    struct tri_compare {
    public:
        int operator()(const test_key& a, const test_key& b) const noexcept {
            return a.tri_cmp(b);
        }
    };

    test_key(int k) : tree_test_key_base(k) {}

    struct invalid{};
    test_key(invalid) : tree_test_key_base(-1) {}

    test_key(test_key&&) = delete;

    bool operator!=(unsigned long o) const noexcept {
        return (unsigned long)(*this) != o;
    }
};

using test_tree = tree<test_key, &test_key::hook>;
using test_validator = validator<test_key, &test_key::hook>;

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
        ("count", bpo::value<int>()->default_value(10000), "number of keys to fill the tree with")
        ("iters", bpo::value<int>()->default_value(13), "number of iterations")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iter = app.configuration()["iters"].as<int>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, iter, verb] {
            test_key::tri_compare cmp;
            std::vector<int> keys;
            for (int i = 0; i < count; i++) {
                keys.push_back(i + 1);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Compacting {:d} k:v pairs {:d} times\n", count, iter);

            test_validator tv;

            logalloc::region mem;

            with_allocator(mem.allocator(), [&] {
                tree_pointer t;

                for (auto rep = 0; rep < iter; rep++) {
                    {
                        std::shuffle(keys.begin(), keys.end(), g);

                        logalloc::reclaim_lock rl(mem);

                        for (int i = 0; i < count; i++) {
                            test_key k(keys[i]);

                            auto ir = t->lower_bound(k, cmp);
                            auto* nk = new test_key(keys[i]);
                            t->insert_before(ir, *nk);
                            seastar::thread::maybe_yield();
                        }
                    }

                    mem.full_compaction();

                    if (verb) {
                        fmt::print("After fill + compact\n");
                        tv.print_tree(*t);
                    }

                    tv.validate(*t, cmp, count);

                    {
                        std::shuffle(keys.begin(), keys.end(), g);

                        logalloc::reclaim_lock rl(mem);

                        for (int i = 0; i < count; i++) {
                            test_key k(keys[i]);

                            bool match;
                            auto ri = t->lower_bound(k, cmp, match);
                            assert(match);
                            ri.erase_and_dispose([] (test_key* k) noexcept { delete k; });
                            seastar::thread::maybe_yield();
                        }
                    }

                    mem.full_compaction();

                    if (verb) {
                        fmt::print("After erase + compact\n");
                        tv.print_tree(*t);
                    }

                    tv.validate(*t, cmp, 0);
                }
            });
        });
    });
}
