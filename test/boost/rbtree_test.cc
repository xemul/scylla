
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

#define BOOST_TEST_MODULE bptree

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include "test/unit/tree_test_key.hh"
#include "utils/intrusive-rbtree.hh"

using namespace intrusive_rb;

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

    test_key(test_key&&) = delete;
};

using test_tree = tree<test_key, &test_key::hook>;

void key_deleter(test_key* k) noexcept {
    delete k;
}

BOOST_AUTO_TEST_CASE(test_lower_bound) {
    test_tree t;
    test_key::tri_compare cmp;
    int nkeys = 42;

    for (int i = 0; i < nkeys; i++) {
        auto* nk = new test_key(2 * i + 1);
        t.insert_before(t.end(), *nk);
    }

    for (int i = 0; i < nkeys; i++) {
        bool match;
        auto it = t.lower_bound(i, cmp, match);

        if (it == t.end()) {
            BOOST_REQUIRE(i == 2 * nkeys);
            break;
        } else if (i % 2 == 0) {
            BOOST_REQUIRE(!match && *it == i + 1);
        } else {
            BOOST_REQUIRE(match && *it == i);
        }
    }

    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_insert_before_hint) {
    test_tree t;
    test_key::tri_compare cmp;
    int nkeys = 29;

    for (int num_keys = 0; num_keys <= nkeys; num_keys++) {
        for (int hint_i = 0; hint_i <= num_keys; hint_i++) {
            for (int i = 0; i < num_keys; i++) {
                auto* nk = new test_key(2 * i + 1);
                t.insert_before(t.end(), *nk);
            }

            auto hint = t.begin();
            for (int i = 0; i < hint_i; i++) {
                hint++;
            }

            for (int i = 0; i < 2 * num_keys + 1; i++) {
                auto nk = std::make_unique<test_key>(i);
                auto npi = t.insert_before_hint(hint, *nk, cmp);
                if (npi.second) {
                    nk.release();
                }
                auto ni = npi.first;
                BOOST_REQUIRE(*ni == i);
                if (hint_i * 2 + 1 == i) {
                    BOOST_REQUIRE(!npi.second);
                    BOOST_REQUIRE(ni == hint);
                }
                if (npi.second) {
                    BOOST_REQUIRE(i % 2 == 0);
                    ni++;
                    if (i == 2 * num_keys) {
                        BOOST_REQUIRE(ni == t.end());
                    } else {
                        BOOST_REQUIRE(*ni == i + 1);
                    }
                } else {
                    BOOST_REQUIRE(i % 2 == 1);
                }
            }

            t.clear_and_dispose(key_deleter);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_singular_tree) {
    test_tree t;
    int nkeys = 7;

    for (int i = 0; i < nkeys; i++) {
        t.insert_before(t.end(), *new test_key(i));
    }

    for (int i = 0; i < nkeys; i++) {
        if (i == nkeys - 1) {
            auto it = t.begin();
            BOOST_REQUIRE(it.tree_if_singular() == &t);
            it.erase_and_dispose(key_deleter);
            break;
        }

        for (auto it = t.begin(); it != t.end(); it++) {
            BOOST_REQUIRE(it.tree_if_singular() == nullptr);
        }

        t.begin().erase_and_dispose(key_deleter);
    }
}

BOOST_AUTO_TEST_CASE(test_range_erase) {
    int size = 32;

    for (int f = 0; f < size; f++) {
        for (int t = f; t < size; t++) {
            test_tree tree;

            for (int i = 0; i < size; i++) {
                tree.insert_before(tree.end(), *new test_key(i));
            }

            auto iter_at = [&tree] (int at) -> typename test_tree::iterator {
                auto it = tree.begin();
                for (int i = 0; i < at; i++, it++);
                return it;
            };

            auto n = tree.erase_and_dispose(iter_at(f), iter_at(t), key_deleter);

            auto r = tree.begin();
            for (int i = 0; i < size; i++) {
                if (!(i >= f && i < t)) {
                    if (i == t) {
                        BOOST_REQUIRE(*n == i);
                    }
                    BOOST_REQUIRE(*(r++) == i);
                }
            }

            if (t == size) {
                BOOST_REQUIRE(n == tree.end());
            }
            BOOST_REQUIRE(r == tree.end());

            tree.clear_and_dispose(key_deleter);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_tree_clone) {
    test_tree t;
    int size = 23;

    for (int nkeys = 0; nkeys < size; nkeys++) {
        for (int i = 0; i < nkeys; i++) {
            t.insert_before(t.end(), *new test_key(i));
        }

        test_tree ct;
        int fail_at = 0;

        while (true) {
            try {
                ct.clone_from(t, [&fail_at] (test_key& k) -> test_key* {
                        int val = k;
                        if (val == fail_at) {
                            throw 0;
                        }
                        return new test_key(val);
                    }, key_deleter);
                break;
            } catch (...) {
                BOOST_REQUIRE(ct.calculate_size() == 0);
                fail_at++;
            }
        }

        auto i = t.begin();
        auto ci = ct.begin();
        auto nk = 0;
        while (true) {
            if (i == t.end()) {
                BOOST_REQUIRE(ci == ct.end());
                break;
            }

            BOOST_REQUIRE(*i == *ci);
            BOOST_REQUIRE(i->hook.is_red() == ci->hook.is_red());
            BOOST_REQUIRE(i->hook.is_black() == ci->hook.is_black()); // %)
            BOOST_REQUIRE(i->hook.is_corner<Left>() == ci->hook.is_corner<Left>());
            BOOST_REQUIRE(i->hook.is_corner<Right>() == ci->hook.is_corner<Right>());
            BOOST_REQUIRE(i->hook.is_root() == ci->hook.is_root());

            i++;
            ci++;
            nk++;
        }

        BOOST_REQUIRE(nk == nkeys);

        t.clear_and_dispose(key_deleter);
        ct.clear_and_dispose(key_deleter);
    }
}
