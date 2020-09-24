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

namespace intrusive_rb {

template <class T, member_hook T::*Link>
class validator {
    using tree_t = tree<T, Link>;
    using key_t = member_hook;

    void show(tree_t& t, key_t* key, int indent) const noexcept {
        if (key == nullptr) {
            return;
        }

        key_t* l = key->maybe_child<Left>();
        key_t* r = key->maybe_child<Right>();
        T inv(typename T::invalid{});
        fmt::print("{:{}}{} {}{}{}{} {}/{}\n", " ", indent, t.to_key(key),
                key->is_red() ? 'r' : 'b',
                key->is_root() ? '^' : '-',
                key->is_corner<Left>() ? '<' : '-',
                key->is_corner<Right>() ? '>' : '-',
                l != nullptr ? t.to_key(l) : inv,
                r != nullptr ? t.to_key(r) : inv);

        show(t, l, indent + 2);
        show(t, r, indent + 2);
    }

    public:
    void print_tree(tree_t& t) const noexcept {
        T inv(typename T::invalid{});
        fmt::print("-------------------------------------\n");
        fmt::print("^{} <{} >{}\n",
                t._base._root != nullptr ? t.to_key(t._base._root) : inv,
                t._base._corners[Left] != nullptr ? t.to_key(t._base._corners[Left]) : inv,
                t._base._corners[Right] != nullptr ? t.to_key(t._base._corners[Right]) : inv);
        show(t, t._base._root, 0);
        fmt::print("-------------------------------------\n");
    }

    private:
    unsigned depth(const key_t* t) const noexcept {
        if (t == nullptr)
            return 1;

        unsigned ldep = depth(t->maybe_child<Left>());
        assert(ldep == depth(t->maybe_child<Right>()));
        return (t->is_black() ? 1 : 0) + ldep;
    }

    template <typename Compare>
    bool valid(tree_t& tree, const key_t* n, const Compare& cmp) const noexcept {
        if (!n)
            return true;

        key_t* l = n->maybe_child<Left>();
        key_t* r = n->maybe_child<Right>();

        if (n->is_red()) {
            if (l && !l->is_black())
                return false;
            if (r && !r->is_black())
                return false;
        }

        if (l != nullptr) {
            if (l->parent() != n) {
                return false;
            }
            if (cmp(tree.to_key(l), tree.to_key(n)) >= 0) {
                return false;
            }
        }

        if (r != nullptr) {
            if (r->parent() != n) {
                return false;
            }
            if (cmp(tree.to_key(r), tree.to_key(n)) <= 0) {
                return false;
            }
        }

        if ((tree._base._corners[Left] == n) != n->is_corner<Left>()) {
            fmt::print("leftmost bit mismatch\n");
            return false;
        }

        if ((tree._base._corners[Right] == n) != n->is_corner<Right>()) {
            fmt::print("rightmost bit mismatch\n");
            return false;
        }

        return valid(tree, l, cmp) && valid(tree, r, cmp);
    }

    template <int Dir, typename Compare>
    bool sorted(tree_t &t, const Compare& cmp, int nkeys) const noexcept {
        key_t* r = t._base._root, *c = r;
        while (true) {
            key_t* n = c->maybe_child<Dir>();
            if (n != nullptr) {
                c = n;
                continue;
            }

            if (t._base._corners[Dir] != c) {
                fmt::print("{} corner mismatch\n", Dir);
                return false;
            }

            break;
        }

        c = t._base._corners[Dir];
        int sz = 0;
        while (true) {
            sz++;
            key_t* n = c->step<Opposite(Dir)>();
            if (n == nullptr) {
                if (sz != nkeys) {
                    fmt::print("iterator++ mismatch {} != {}\n", nkeys, sz);
                    return false;
                }
                break;
            }

            auto x = cmp(t.to_key(c), t.to_key(n));
            if ((Dir == Left && x >= 0) || (Dir == Right && x <= 0)) {
                fmt::print("{} direction misorder\n", Dir);
                return false;
            }
            c = n;
        }

        return true;
    }

    template <typename Compare>
    bool valid(tree_t& t, const Compare& cmp, int nkeys) const noexcept {
        if (int sz = t.calculate_size(); nkeys != sz) {
            fmt::print("size mismatch {} != {}\n", nkeys, sz);
            return false;
        }

        if (t.empty()) {
            if (t._base._corners[Left] != nullptr || t._base._corners[Right] != nullptr) {
                fmt::print("left/right not null\n");
                return false;
            }
            return true;
        }

        if (!sorted<Left>(t, cmp, nkeys)) {
            return false;
        }

        if (!sorted<Right>(t, cmp, nkeys)) {
            return false;
        }

        key_t* r = t._base._root;

        if (!r->is_black()) {
            fmt::print("root is not black\n");
            return false;
        }
        if (r->root_tree() != &t._base) {
            fmt::print("root link broken\n");
            return false;
        }
        if (depth(r->maybe_child<Left>()) != depth(r->maybe_child<Right>())) {
            fmt::print("black-depths mismatch\n");
            return false;
        }

        return valid(t, t._base._root, cmp);
    }

public:
    template <typename Compare>
    void validate(tree_t& t, const Compare& cmp, int nkeys) const {
        if (!valid(t, cmp, nkeys)) {
            throw "tree is broken";
        }
    }
};

}
