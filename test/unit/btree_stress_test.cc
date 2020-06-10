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
#include <fmt/ostream.h>

constexpr int TEST_NODE_SIZE = 8;

#include "utils/intrusive_btree.hh"
#include "btree_validation.hh"
#include "test/unit/tree_test_key.hh"

using namespace intrusive_b;
using namespace seastar;

class test_key : public tree_test_key_base {
public:
    member_hook _hook;
    test_key(int nr) noexcept : tree_test_key_base(nr) {}
    test_key(const test_key&) = delete;
    test_key(test_key&&) = delete;
};

using test_tree = tree<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, key_search::both, with_debug::yes>;
using test_validator = validator<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE>;
using test_iterator_checker = iterator_checker<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE>;

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(4132), "number of keys to fill the tree with")
        ("iters", bpo::value<int>()->default_value(9), "number of iterations")
        ("keys",  bpo::value<std::string>()->default_value("rand"), "how to generate keys (rand, asc, desc)")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iters = app.configuration()["iters"].as<int>();
        auto ks = app.configuration()["keys"].as<std::string>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, iters, ks, verb] {
            int rep = iters;
            auto *t = new test_tree();
            test_key_tri_compare cmp;
            std::map<int, unsigned long> oracle;

            int p = count / 10;
            if (p == 0) {
                p = 1;
            }

            std::vector<int> keys;

            for (int i = 0; i < count; i++) {
                keys.push_back(i + 1);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Inserting {:d} k:v pairs {:d} times\n", count, rep);

            test_validator tv;

            if (ks == "desc") {
                fmt::print("Reversing keys vector\n");
                std::reverse(keys.begin(), keys.end());
            }

            bool shuffle = ks == "rand";
            if (shuffle) {
                fmt::print("Will shuffle keys each iteration\n");
            }


        again:
            auto* itc = new test_iterator_checker(tv, *t);

            if (shuffle) {
                std::shuffle(keys.begin(), keys.end(), g);
            }

            for (int i = 0; i < count; i++) {
                test_key *k = new test_key(keys[i]);

                if (verb) {
                    fmt::print("+++ {}\n", (int)*k);
                }

                auto ir = t->insert(*k, cmp);
                assert(ir.second);
                oracle[keys[i]] = keys[i]; // BPTREE

                if (verb) {
                    fmt::print("Validating\n");
                    tv.print_tree(*t, '|');
                }

                /* Limit validation rate for many keys */
                if (i % (i/1000 + 1) == 0) {
                    tv.validate(*t);
                }

                if (i % 7 == 0) {
                    if (!itc->step()) {
                        delete itc;
                        itc = new test_iterator_checker(tv, *t);
                    }
                }

                seastar::thread::maybe_yield();
            }

            auto ti = t->begin();
            for (auto oe : oracle) {
                if ((unsigned int)*ti != oe.second) {
                    fmt::print("Data mismatch {} vs {}\n", oe.second, *ti);
                    throw "oracle";
                }
                ti++;
            }

            if (shuffle) {
                std::shuffle(keys.begin(), keys.end(), g);
            }

            auto deleter = [] (test_key* k) noexcept { delete k; };

            for (int i = 0; i < count; i++) {
                test_key k(keys[i]);

                /*
                 * kill iterator if we're removing what it points to,
                 * otherwise it's not invalidated
                 */
                if (itc->here(k)) {
                    delete itc;
                    itc = nullptr;
                }

                if (verb) {
                    fmt::print("--- {}\n", (int)k);
                }

                t->erase_and_dispose(k, cmp, deleter);

                oracle.erase(keys[i]);

                if (verb) {
                    fmt::print("Validating\n");
                    tv.print_tree(*t, '|');
                }

                if ((count-i) % ((count-i)/1000 + 1) == 0) {
                    tv.validate(*t);
                }

                if (itc == nullptr) {
                    itc = new test_iterator_checker(tv, *t);
                }

                if (i % 5 == 0) {
                    if (!itc->step()) {
                        delete itc;
                        itc = new test_iterator_checker(tv, *t);
                    }
                }

                seastar::thread::maybe_yield();
            }

            delete itc;

            if (--rep > 0) {
                if (verb) {
                    fmt::print("{:d} iterations left\n", rep);
                }
                goto again;
            }

            oracle.clear();
            delete t;
        });
    });
}
