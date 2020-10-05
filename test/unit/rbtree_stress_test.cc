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
            test_key::tri_compare cmp;
            auto t = std::make_unique<test_tree>();
            std::map<int, unsigned long> oracle;

            std::vector<int> keys;

            for (int i = 0; i < count; i++) {
                keys.push_back(i + 1);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Inserting {:d} k:v pairs {:d} times\n", count, iters);

            test_validator tv;

            if (ks == "desc") {
                fmt::print("Reversing keys vector\n");
                std::reverse(keys.begin(), keys.end());
            }

            bool shuffle = ks == "rand";
            if (shuffle) {
                fmt::print("Will shuffle keys each iteration\n");
            }


            for (auto rep = 0; rep < iters; rep++) {
                if (verb) {
                    fmt::print("Iteration {:d}\n", rep);
                }

                if (shuffle) {
                    std::shuffle(keys.begin(), keys.end(), g);
                }

                for (int i = 0; i < count; i++) {
                    test_key k(keys[i]);

                    if (verb) {
                        fmt::print("+++ {}\n", (int)k);
                    }

                    auto ir = t->lower_bound(k, cmp);
                    t->insert_before(ir, *new test_key(keys[i]));
                    oracle[keys[i]] = keys[i];

                    if (verb) {
                        fmt::print("Validating\n");
                        tv.print_tree(*t);
                    }

                    if (i % (i/1000 + 1) == 0) {
                        tv.validate(*t, cmp, i + 1);
                    }

                    seastar::thread::maybe_yield();
                }

                auto sz = t->calculate_size();
                if (sz != (size_t)count) {
                    fmt::print("Size {} != count {}\n", sz, count);
                    throw "size";
                }

                auto ti = t->begin();
                for (auto oe : oracle) {
                    if (*ti != oe.second) {
                        fmt::print("Data mismatch {} vs {}\n", oe.second, *ti);
                        throw "oracle";
                    }
                    ti++;
                }

                if (shuffle) {
                    std::shuffle(keys.begin(), keys.end(), g);
                }

                for (int i = 0; i < count; i++) {
                    test_key k(keys[i]);

                    if (verb) {
                        fmt::print("--- {}\n", (int)k);
                    }

                    bool match;
                    auto ri = t->lower_bound(k, cmp, match);
                    assert(match);
                    ri.erase_and_dispose([] (test_key* k) noexcept { delete k; });

                    oracle.erase(keys[i]);

                    if (verb) {
                        fmt::print("Validating\n");
                        tv.print_tree(*t);
                    }

                    if ((count-i) % ((count-i)/1000 + 1) == 0) {
                        tv.validate(*t, cmp, count - i - 1);
                    }

                    seastar::thread::maybe_yield();
                }
            }
        });
    });
}
