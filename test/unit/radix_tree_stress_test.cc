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

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(132564), "number of indices to fill the tree with")
        ("sparse", bpo::value<int>()->default_value(3), "sparse factor (max index value will be count * sparse)")
        ("iters", bpo::value<int>()->default_value(5), "number of iterations")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto sparse = std::max(app.configuration()["sparse"].as<int>(), 1);
        auto iters = app.configuration()["iters"].as<int>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, sparse, iters, verb] {
            auto t = std::make_unique<test_tree>();
            std::map<unsigned, test_data> oracle;

            int p = count / 10;
            if (p == 0) {
                p = 1;
            }

            std::vector<unsigned> keys;

            for (int i = 0; i < count * sparse; i++) {
                keys.push_back(i);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Inserting {} indices (sparse {}) {} times\n", count, sparse, iters);

            for (auto rep = 0; rep < iters; rep++) {
                if (verb) {
                    fmt::print("Iteration {:d}\n", rep);
                }

                if (rep != 0) {
                    // 0th iteration goes with linear keys
                    std::shuffle(keys.begin(), keys.end(), g);
                }

                for (int i = 0; i < count * sparse; i++) {
                    if (verb) {
                        fmt::print("+++ {}\n", keys[i]);
                    }

                    t->emplace(keys[i], keys[i]);
                    oracle.emplace(std::make_pair(keys[i], keys[i]));
                    if (verb) {
                        compact_radix_tree::printer<test_data, unsigned>::show(*t);
                    }

                    seastar::thread::maybe_yield();
                }

                if (rep != 0) {
                    std::shuffle(keys.begin(), keys.end(), g);
                }

                for (int i = 0; i < count * (sparse - 1); i++) {
                    if (verb) {
                        fmt::print("--- {}\n", keys[i]);
                    }

                    t->erase(keys[i]);
                    oracle.erase(keys[i]);
                    if (verb) {
                        compact_radix_tree::printer<test_data, unsigned>::show(*t);
                    }

                    seastar::thread::maybe_yield();
                }

                for (auto&& d : oracle) {
                    test_data* td = t->get(d.first);
                    assert(td != nullptr);
                    assert(td->value() == d.second.value());
                }

                int nr = 0;
                auto ti = t->begin();
                while (ti != t->end()) {
                    assert(ti->value() == ti.index());
                    nr++;
                    ti++;
                }
                assert(nr == count);

                nr = 0;
                t->walk([&nr, count] (unsigned idx, test_data& td) {
                    assert(idx == td.value());
                    nr++;
                    return true;
                });
                assert(nr == count);

                nr = 0;
                unsigned idx = 0;
                while (true) {
                    test_data* td = t->lower_bound(idx);
                    if (td == nullptr) {
                        break;
                    }
                    assert(td->value() >= idx);
                    nr++;
                    idx = td->value() + 1;
                }
                assert(nr == count);

                t->clear();
                oracle.clear();
            }
        });
    });
}

namespace compact_radix_tree {
template<>
test_tree::node_head test_tree::nil_root = test_tree::node_head();
}
