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

#include <functional>
#include <cassert>
#include <boost/intrusive/parent_from_member.hpp>
#include "utils/collection-concepts.hh"
#include "utils/binary-pointer.hh"
#include "utils/neat-object-id.hh"

namespace intrusive_rb {

template <typename Func, typename T>
concept KeyCloner = requires (Func f, T& val) {
    { f(val) } -> std::same_as<T*>;
};

class member_hook;
template <class T, member_hook T::*Link> class tree;
template <class T, member_hook T::*Link>  class validator;

static constexpr int Left = 0;
static constexpr int Right = 1;
static constexpr int Opposite(int dir) { return 1 - dir; }

/*
 * Header of the tree. It's templates free, because the member_hook is
 * such and it's corner instances have pointers on the tree_base.
 */
class tree_base {
    template <class T, member_hook T::*L> friend class tree;
    template <class T, member_hook T::*L> friend class validator;
    friend class member_hook;

    member_hook* _root;
    member_hook* _corners[2]; // The left-most and the right-most keys in the tree

    tree_base() noexcept : _root(nullptr), _corners{nullptr, nullptr} { }
    tree_base(const tree_base&) = delete;

    ~tree_base() {
        assert(_root == nullptr);
        assert(_corners[Left] == nullptr);
        assert(_corners[Right] == nullptr);
    }
};

/*
 * Structure to be embedded into the key. Has 3 pointers (parent, left and
 * right), but it's corner instances (root, left- and right-most) point to
 * the tree_base.
 */
class member_hook {
    template <class T, member_hook T::*L> friend class tree;
    template <class T, member_hook T::*L> friend class validator;
    friend class tree_base;

    using parent_pointer_t = utils::binary_pointer<member_hook, tree_base, true>;
    using child_pointer_t = utils::binary_pointer<member_hook, tree_base, false>;

    /*
     * Pointer to parent key or, for root key, to the tree_base.
     * Notch is the color, default is red.
     */
    parent_pointer_t _parent_or_tree;

    /*
     * Left and right childred. For left-most and right-most (in the
     * tree scope) these point to the tree_base.
     */
    child_pointer_t _child_or_tree[2];

    [[no_unique_address]] utils::neat_id<false> _id;

public:

    member_hook() noexcept {
        parent_pointer_t::check_aligned(this);
    }

    member_hook(const member_hook&) = delete;
    member_hook(member_hook&& other) noexcept {
        bool black = other.is_black();

        other.relink_parent_to(this);
        other.relink_child_to<Left>(this);
        other.relink_child_to<Right>(this);
        other.clear();

        if (black) {
            paint_black();
        }
    }

    ~member_hook() {
        assert(!_parent_or_tree);
        assert(!_child_or_tree[Left]);
        assert(!_child_or_tree[Right]);
    }

    bool is_black() const noexcept { return _parent_or_tree.notched(); }
    bool is_red() const noexcept { return !is_black(); }

    bool is_root() const noexcept {
        return _parent_or_tree.template is<tree_base>();
    }

    // Partial is the key that has zero or one child.
    // Needed for removing a key from tree.
    bool is_partial() const noexcept {
        return maybe_child<Left>() == nullptr || maybe_child<Right>() == nullptr;
    }

    template <int Dir>
    bool is_my_child(const member_hook* o) const noexcept {
        return _child_or_tree[Dir] == child_pointer_t(o);
    }

    // Checks if it's the left-/right-most key in the tree
    template <int Dir>
    bool is_corner() const noexcept {
        return _child_or_tree[Dir].is<tree_base>();
    }

private:
    // Artworks
    void repaint() noexcept { _parent_or_tree.invert_notch(); }
    void paint_red() noexcept { _parent_or_tree.denotch(); }
    void paint_black() noexcept { _parent_or_tree.notch(); }

    // Gets the parent key. For root key returns nullptr
    member_hook* parent() const noexcept {
        return _parent_or_tree.template as<member_hook>();
    }

    // Gets left/right child. For corner keys returns nullptr
    template <int Dir>
    member_hook* maybe_child() const noexcept {
        return _child_or_tree[Dir].maybe_as<member_hook>();
    }

    // Gets left/right child. To be used when the child is known
    // to be child, not corner tree.
    template <int Dir>
    member_hook* child() const noexcept {
        return _child_or_tree[Dir].as<member_hook>();
    }

    // Gets the tree_base of the root. Key must be root
    tree_base* root_tree() const noexcept {
        return _parent_or_tree.template as<tree_base>();
    }

    // Gets the tree_base of the corner key. Key must be such
    template <int Dir>
    tree_base* corner_tree() const noexcept {
        return _child_or_tree[Dir].as<tree_base>();
    }

    // Gets the other key of this key's parent
    member_hook* sibling_of(const member_hook* o) const noexcept {
        return is_my_child<Left>(o) ? maybe_child<Right>() : maybe_child<Left>();
    }

    // Sets @c as this' child. If the @c is not null, also sets
    // its parent pointer
    template <int Dir>
    void set_child(member_hook* c) noexcept {
        if (c != nullptr) {
            c->_parent_or_tree.set(this);
        }
        _child_or_tree[Dir].set(c);
    }

    // Replaces left/right child with the new one
    void replace_child(const member_hook& o, member_hook* n) noexcept {
        if (is_my_child<Left>(&o)) {
            set_child<Left>(n);
        } else {
            set_child<Right>(n);
        }
    }

    // Replaces the parent's (or tree's) pointer on this with the
    // new value.
    void relink_parent_to(member_hook* n) noexcept {
        if (is_root()) {
            tree_base* t = root_tree();
            t->_root = n;
            if (n != nullptr) {
                n->_parent_or_tree.set(t);
            }
        } else {
            parent()->replace_child(*this, n);
        }
    }

    template <int Dir>
    void relink_child_to(member_hook* n) noexcept {
        if (is_corner<Dir>()) {
            tree_base* t = corner_tree<Dir>();
            t->_corners[Dir] = n;
            if (n != nullptr) {
                n->_child_or_tree[Dir].set(t);
            }
        } else {
            n->set_child<Dir>(child<Dir>());
        }
    }

    // Returns the sub-tree's left-/right-most key. Can only be
    // used after the opposite step, so that the node we're looking
    // for is for sure not the corner one.
    template <int Dir>
    member_hook* inner_corner() const noexcept {
        const member_hook* n = this, *c;
        while ((c = n->child<Dir>()) != nullptr) {
            n = c;
        }
        return const_cast<member_hook*>(n);
    }

    // Returns the next/prev key in the tree
    template <int Dir>
    member_hook* step() const noexcept {
        if (member_hook* r = maybe_child<Dir>(); r != nullptr) {
            return r->inner_corner<Opposite(Dir)>();
        }

        const member_hook* n = this;
        while (!n->is_root()) {
            member_hook* p = n->parent();
            if (p->is_my_child<Opposite(Dir)>(n)) {
                return p;
            }
            n = p;
        }

        return nullptr;
    }

    void clear() noexcept {
        _child_or_tree[Left].reset();
        _child_or_tree[Right].reset();
        _parent_or_tree.reset();
    }

    /*
     * Balancing was coded from
     * "Introduction to Algorithms" by Hormen, Leiserson and Rivest
     *
     * The code coments below refer to the cases as they are marked
     * in the book, variables are named the same.
     */

    template <int Dir>
    static void rotate(member_hook& p) noexcept {
        /*
         * Right:            Left:
         *
         *   |      |         |           |
         *  (p)     n        (p)          n
         *  /        \         \         /
         * n    >>   (p)        n  >>  (p)
         *  \        /         /         \
         *   c      c         c           c
         *
         */
        member_hook* n = p.child<Opposite(Dir)>();
        member_hook* c = n->child<Dir>();

        p.relink_parent_to(n);
        n->set_child<Dir>(&p);
        p.set_child<Opposite(Dir)>(c);
    }

    template <int Dir>
    static void swap_child(member_hook& foo, member_hook& bar) noexcept {
        member_hook* fc = foo.maybe_child<Dir>();
        member_hook* bc = bar.maybe_child<Dir>();

        foo.set_child<Dir>(bc);
        bar.set_child<Dir>(fc);
    }

    static void swap_nodes(member_hook& foo, member_hook& bar) noexcept {
        assert(&bar != &foo);

        member_hook* bp = bar.parent();
        foo.relink_parent_to(&bar);
        bp->replace_child(bar, &foo);

        swap_child<Left>(foo, bar);
        swap_child<Right>(foo, bar);

        if (foo.is_black() != bar.is_black()) {
            bar.repaint();
            foo.repaint();
        }
    }

    static void balance_on_erase(member_hook* x) noexcept {
        for (;;) {
            if (x->is_root()) {
                break;
            }

            member_hook* p = x->parent();
            member_hook* w = p->sibling_of(x);

            if (is_red(w)) {
                // Case 1
                p->paint_red();
                w->paint_black();

                if (p->is_my_child<Left>(x)) {
                    rotate<Left>(*p);
                } else {
                    rotate<Right>(*p);
                }

                p = x->parent(); // update after rotation
                w = p->sibling_of(x);
            }

            member_hook* wl = w ? w->maybe_child<Left>() : nullptr;
            member_hook* wr = w ? w->maybe_child<Right>() : nullptr;

            if (is_black(wl) && is_black(wr)) {
                // Case 2
                w->paint_red();

                if (is_red(p)) {
                    p->paint_black();
                    break;
                }

                x = p; // recursion-less return balance_on_erase(p)
                continue;
            }

            if (p->is_my_child<Left>(x) && is_black(wr)) {
                // Case 3
                w->paint_red();
                wl->paint_black();
                rotate<Right>(*w);
            } else if (p->is_my_child<Right>(x) && is_black(wl)) {
                // Case 3 (symmetrical)
                w->paint_red();
                wr->paint_black();
                rotate<Left>(*w);
            }

            // Case 4
            p = x->parent();
            w = p->sibling_of(x);

            if (is_red(p)) {
                p->paint_black();
                w->paint_red();
            }

            if (p->is_my_child<Left>(x)) {
                w->child<Right>()->paint_black();
                rotate<Left>(*p);
            } else {
                w->child<Left>()->paint_black();
                rotate<Right>(*p);
            }

            break;
        }
    }

    static void balance_on_insert(member_hook *x, member_hook *p) noexcept {
        for (;;) {
            if (p->is_black() || p->is_root()) {
                break;
            }

            member_hook* g = p->parent();
            member_hook* u = g->sibling_of(p);
            if (is_red(u)) {
                // Case 1
                g->paint_red();
                p->paint_black();
                u->paint_black();

                if (g->is_root()) {
                    g->paint_black();
                    break;
                }

                x = g; // recursion-less return balance_on_insert(g)
                p = g->parent();
                continue;
            }

            if (p->is_my_child<Right>(x) && g->is_my_child<Left>(p)) {
                // Case 2
                rotate<Left>(*p);
            } else if (p->is_my_child<Left>(x) && g->is_my_child<Right>(p)) {
                // Case 2 (symmetrical)
                rotate<Right>(*p);
            } else {
                /*
                 * Not-so-obsious trick to match the classical algo.
                 * Case 2 continues into case 3 with x set to p, thus
                 * x = p here. At the same time, the below rotations
                 * depend on whether the x is left or right child of 
                 * p so p = x to reuse the pointer.
                 */
                std::swap(x, p);
            }

            // Case 3
            if (p->parent()->is_my_child<Left>(p)) {
                rotate<Right>(*g);
            } else {
                rotate<Left>(*g);
            }

            g->paint_red();
            x->paint_black();

            break;
        }
    }

    /*
     * The rb-tree chapter of the book introduces the concept of
     * 'sentinels' not to special-case the nil childs. This code
     * keeps nullptrs instead, so check them by hands or by the
     * is_black/_red(key) helpers
     */
    static bool is_red(const member_hook* n) noexcept { return n && n->is_red(); }
    static bool is_black(const member_hook* n) noexcept { return !n || n->is_black(); }

    void insert(member_hook* parent, int dir, tree_base& base) noexcept {
        _parent_or_tree = parent_pointer_t(parent);
        if (parent == nullptr) {
            assert(base._root == nullptr);
            base._corners[Left] = this;
            base._corners[Right] = this;
            base._root = this;
            _child_or_tree[Left].set(&base);
            _child_or_tree[Right].set(&base);
            _parent_or_tree.set(&base);
            paint_black();
        } else {
            parent->_child_or_tree[dir] = child_pointer_t(this);
            if (base._corners[dir] == parent) {
                base._corners[dir] = this;
                _child_or_tree[dir] = child_pointer_t(&base);
            }

            balance_on_insert(this, parent);
        }
    }

    void erase(member_hook* next) noexcept {
        tree_base* tree = nullptr;
        std::optional<member_hook*> new_left, new_right;

        /*
         * The tree can be balanced in the end, while doing this
         * the keys' left/right pointers will be tossed, so not to
         * make the balancer think of it, keep the right-/left-
         * most key here and update the tree at the end.
         */
        if (is_corner<Left>()) {
            new_left = step<Right>();
            tree = corner_tree<Left>();
            _child_or_tree[Left].reset();
        }
        if (is_corner<Right>()) {
            new_right = step<Left>();
            tree = corner_tree<Right>();
            _child_or_tree[Right].reset();
        }

        if (!is_partial()) {
            /*
             * Key with both children. In this case we get the
             * next (') one and swap them. The tree becomes unsorted
             * at this point, but the "new partial next" is removed
             * right at once.
             *
             * (') it could be prev either, but we already have next
             * at hands and getting prev will require 2nd tree dive,
             * so just next
             */
            if (next->is_corner<Right>()) {
                assert(!new_right);
                new_right = next;
                tree = next->corner_tree<Right>();
            } else {
                assert(next->is_partial());
            }

            swap_nodes(*this, *next);
        }

        erase_partial();

        if (new_left) {
            member_hook* nl = *new_left;
            tree->_corners[Left] = nl;
            if (nl != nullptr) {
                nl->_child_or_tree[Left].set(tree);
            }
        }
        if (new_right) {
            member_hook* nr = *new_right;
            tree->_corners[Right] = nr;
            if (nr != nullptr) {
                nr->_child_or_tree[Right].set(tree);
            }
        }
    }

    void erase_partial() noexcept {
        member_hook* c = nullptr;

        /*
         * Pick up a child (if it exists) and replace current node
         * with it, rebalancing if needed.
         */

        if (_child_or_tree[Left]) {
            assert(maybe_child<Right>() == nullptr);
            c = maybe_child<Left>();
            _child_or_tree[Left].reset();
        } else if (_child_or_tree[Right]) {
            c = maybe_child<Right>();
            _child_or_tree[Right].reset();
        }

        if (c != nullptr) {
            c->paint_black();
        } else if (is_black()) {
            balance_on_erase(this);
        }

        relink_parent_to(c); // c can be nullptr
        _parent_or_tree.reset();
    }

public:
    size_t calculate_size() const noexcept {
        return 1 +
            (maybe_child<Left>() == nullptr ? 0 : child<Left>()->calculate_size()) +
            (maybe_child<Right>() == nullptr ? 0 : child<Right>()->calculate_size());
    }
};

// For .{do_something_with_data}_and_dispose methods below
template <typename T>
void default_dispose(T* value) noexcept { }

/*
 * The main tree class. "Knows" about the Key type and its hook.
 * The comparator is not kept, but is expected to be always
 * provided in the needed places.
 */
template <class Key, member_hook Key::*Link>
class tree {
    template <class T, member_hook T::*L> friend class validator;
    tree_base _base;

public:
    class iterator;
    class const_iterator;

    tree() noexcept : _base() {
        member_hook::parent_pointer_t::check_aligned(&_base);
    }

    tree(const tree&) noexcept = default;
    tree(tree&& other) noexcept {
        _base._root = std::exchange(other._base._root, nullptr);
        if (_base._root != nullptr) {
            _base._root->_parent_or_tree.set(&_base);
        }
        set_corner<Left>(std::exchange(other._base._corners[Left], nullptr));
        set_corner<Right>(std::exchange(other._base._corners[Right], nullptr));
    }

    bool empty() const noexcept {
        return _base._root == nullptr;
    }

    iterator insert_before(iterator pos, Key& key, std::optional<iterator> prev = {}) noexcept {
        member_hook* parent;
        int dir;

        if (pos == end()) {
            /*
             * Inserting before end().
             *
             * tree
             *  / \
             *  ...
             *      \     insert to the right of the
             *       R <- tree's right-most corner
             *
             * Handles the empty-tree case too.
             */
            parent = _base._corners[Right];
            dir = Right;
        } else {
            /*
             * Inserting before a specific key.
             *
             *  (k) - no left child
             *  / \
             * .   r
             * ^
             *  `--- insert as pos's left child
             *
             *
             *    (k) - has left child
             *    / \
             *   l   r
             *  / \
             * .....      insert right after the pos's
             *      \     predecessor, which is l's right
             *       R <- most corner
             *
             */
            parent = &((*pos).*Link);
            dir = Left;
            if (member_hook* l = parent->maybe_child<Left>(); l != nullptr) {
                parent = prev ? &((**prev).*Link) : l->inner_corner<Right>();
                dir = Right;
            }
        }

        (key.*Link).insert(parent, dir, _base);
        return iterator(key.*Link);
    }

    template <typename Compare>
    requires Comparable<Key, Key, Compare>
    std::pair<iterator, bool> insert_before_hint(iterator hint, Key& key, Compare cmp) {
        int x = -1;

        /*
         * Check if the key is left to the hint. If the
         * hint is end(), then it's true in advance.
         */
        if (hint != end()) {
            x = cmp(key, *hint);
            if (x == 0) {
                return std::pair(iterator(hint), false);
            }
        }

        if (x < 0) {
            x = 1;
            iterator prev = end();

            /*
             * Check if key is right to the hint's prev. If
             * hint is the begin, it's true in advance.
             */

            if (hint != begin()) {
                prev = std::prev(hint);
                x = cmp(key, *prev);
                if (x == 0) {
                    return std::pair(iterator(prev), false);
                }
            }

            if (x > 0) {
                /*
                 * Go and do the regular insert_before, but give
                 * it a hint about the previous key not to make
                 * it walk down the tree for the 2nd time.
                 */
                return std::pair(insert_before(hint, key, prev), true);
            }
        }

        /*
         * No luck -- go and find its place in the tree
         */
        bool match;
        iterator next = lower_bound(key, std::move(cmp), match);
        if (match) {
            return std::pair(next, false);
        } else {
            /*
             * XXX: the lower_bound had walked the tree already, maybe it
             * could have met the prev one to push it as a hint here?
             */
            return std::pair(insert_before(next, key), true);
        }
    }

    Key* unlink_leftmost_without_rebalance() noexcept {
        member_hook* l = _base._corners[Left];
        if (l == nullptr) {
            _base._corners[Right] = nullptr;
            return nullptr;
        }

        assert(l->maybe_child<Left>() == nullptr);
        member_hook* n = l->step<Right>();
        l->relink_parent_to(l->maybe_child<Right>());
        set_corner<Left>(n);

        l->clear();
        return &to_key(l);
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    const_iterator lower_bound(const K& k, Compare cmp, bool& match) const {
        member_hook::child_pointer_t cur(_base._root), ret(&_base);
        member_hook* n;

        while ((n = cur.maybe_as<member_hook>()) != nullptr) {
            int i = cmp(k, to_key(n));
            if (i == 0) {
                match = true;
                return const_iterator(*n);
            }

            if (i < 0) {
                ret = cur;
                cur = n->_child_or_tree[Left];
            } else {
                cur = n->_child_or_tree[Right];
            }
        }

        match = false;
        return const_iterator(ret);
    }

    const tree* const_this() noexcept { return const_cast<const tree*>(this); }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    const_iterator lower_bound(const K& k, Compare cmp) const {
        bool match;
        return lower_bound(k, std::move(cmp), match);
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    iterator lower_bound(const K& k, Compare cmp, bool& match) {
        return iterator(const_this()->lower_bound(k, std::move(cmp), match));
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    iterator lower_bound(const K& k, Compare cmp) {
        bool match;
        return lower_bound(k, std::move(cmp), match);
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    const_iterator upper_bound(const K& k, Compare cmp) const {
        bool match;
        const_iterator ret = lower_bound(k, std::move(cmp), match);
        if (match) {
            ret++;
        }
        return ret;
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    iterator upper_bound(const K& k, Compare cmp) {
        return iterator(const_this()->upper_bound(k, std::move(cmp)));
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    const_iterator find(const K& k, Compare cmp) const {
        bool match;
        const_iterator ret = lower_bound(k, std::move(cmp), match);
        return match ? ret : cend();
    }

    template <typename K, typename Compare>
    requires Comparable<K, Key, Compare>
    iterator find(const K& k, Compare cmp) {
        return iterator(const_this()->find(k, std::move(cmp)));
    }

    template <typename Disp>
    requires Disposer<Disp, Key>
    void clear_and_dispose(Disp&& disp) noexcept {
        while (true) {
            Key* k = unlink_leftmost_without_rebalance();
            if (k == nullptr) {
                break;
            }
            disp(k);
        }
    }

    void clear() noexcept {
        clear_and_dispose(default_dispose<Key>);
    }

    template <typename Disp>
    requires Disposer<Disp, Key>
    iterator erase_and_dispose(iterator it, Disp&& disp) noexcept {
        return it.erase_and_dispose(disp);
    }

    template <typename Disp>
    requires Disposer<Disp, Key>
    iterator erase_and_dispose(iterator from, iterator to, Disp&& disp) noexcept {
        while (from != to) {
            from = from.erase_and_dispose(disp);
        }
        return from;
    }

    template <typename... Args>
    iterator erase(Args&&... args) noexcept {
        return erase_and_dispose(std::forward<Args>(args)..., default_dispose<Key>);
    }

    template <typename Cloner, typename Deleter>
    requires KeyCloner<Cloner, Key> && Disposer<Deleter, Key>
    void clone_from(const tree& t, Cloner&& cloner, Deleter&& deleter) {
        clear_and_dispose(deleter);

        if (t.empty()) {
            return;
        }

        auto x_cloner = [cloner = std::move(cloner)] (member_hook* k) -> member_hook* {
            return &(cloner(to_key(k))->*Link);
        };

        member_hook* cur = t._base._root;
        member_hook* left = nullptr;
        member_hook* right = nullptr;

        try {
            member_hook* cur_c = x_cloner(cur);
            cur_c->paint_black();
            _base._root = cur_c;
            cur_c->_parent_or_tree.set(&_base);
            left = cur_c;

            while (true) {
                if (cur->maybe_child<Left>() != nullptr && cur_c->maybe_child<Left>() == nullptr) {
                    cur = cur->child<Left>();
                    cur_c->set_child<Left>(x_cloner(cur));
                    cur_c = cur_c->child<Left>();
                    if (cur->is_black()) {
                        cur_c->paint_black();
                    }
                    if (right == nullptr) {
                        left = cur_c;
                    }
                } else if (cur->maybe_child<Right>() != nullptr && cur_c->maybe_child<Right>() == nullptr) {
                    cur = cur->child<Right>();
                    cur_c->set_child<Right>(x_cloner(cur));
                    cur_c = cur_c->child<Right>();
                    if (cur->is_black()) {
                        cur_c->paint_black();
                    }
                    right = cur_c;
                } else if (cur != t._base._root) {
                    cur = cur->parent();
                    cur_c = cur_c->parent();
                } else {
                    set_corner<Left>(left == nullptr ? cur_c : left);
                    set_corner<Right>(right == nullptr ? cur_c : right);
                    break;
                }
            }
        } catch (...) {
            set_corner<Left>(left); // for clearing
            clear_and_dispose(deleter);
            throw;
        }
    }

    size_t calculate_size() const noexcept {
        return _base._root == nullptr ? 0 : _base._root->calculate_size();
    }

private:
    static const Key& to_key(const member_hook* kl) noexcept {
        return *boost::intrusive::get_parent_from_member(kl, Link);
    }
    static Key& to_key(member_hook* kl) noexcept {
        return *boost::intrusive::get_parent_from_member(kl, Link);
    }

    // A helper to link corner key with the tree
    template <int Dir>
    void set_corner(member_hook* n) noexcept {
        _base._corners[Dir] = n;
        if (n != nullptr) {
            n->_child_or_tree[Dir].set(&_base);
        }
    }

public:
    template <bool Const>
    class iterator_base {
        using pointer_t = std::conditional_t<Const, const member_hook::child_pointer_t, member_hook::child_pointer_t>;
        using iter_key_t = std::conditional_t<Const, const member_hook, member_hook>;
        using iter_tree_t = std::conditional_t<Const, const tree_base, tree_base>;
    protected:
        member_hook::child_pointer_t _key_or_tree;

        iterator_base() noexcept = default;
        iterator_base(iter_key_t& key) noexcept : _key_or_tree(&key) {}
        iterator_base(iter_tree_t& tree) noexcept : _key_or_tree(&tree) {}
        iterator_base(pointer_t& bp) noexcept : _key_or_tree(bp) {}

    public:
        template <bool C> friend class iterator_base;
        iterator_base(const iterator_base<true>& o) noexcept : _key_or_tree(o._key_or_tree) {}
        iterator_base(const iterator_base<false>& o) noexcept : _key_or_tree(o._key_or_tree) {}

        iterator_base& operator++() noexcept {
            iter_key_t& k = *_key_or_tree.template as<member_hook>();
            if (k.template is_corner<Right>()) {
                _key_or_tree.set(k.template corner_tree<Right>());
            } else {
                _key_or_tree.set(k.template step<Right>());
            }
            return *this;
        }

        iterator_base operator++(int) noexcept {
            iterator_base cur = *this;
            operator++();
            return cur;
        }

        iterator_base& operator--() noexcept {
            if (_key_or_tree.template is<tree_base>()) {
                iter_tree_t& t = *_key_or_tree.template as<tree_base>();
                _key_or_tree.set(t._corners[Right]);
            } else {
                iter_key_t& k = *_key_or_tree.template as<member_hook>();
                _key_or_tree.set(k.template step<Left>());
            }
            return *this;
        }

        iterator_base operator--(int) noexcept {
            iterator_base cur = *this;
            operator--();
            return cur;
        }

        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = std::conditional_t<Const, const Key, Key>;
        using difference_type = ssize_t;
        using pointer = value_type*;
        using reference = value_type&;

        reference operator*() const noexcept {
            return tree::to_key(_key_or_tree.template as<member_hook>());
        }
        pointer operator->() const noexcept {
            return &tree::to_key(_key_or_tree.template as<member_hook>());
        }

        bool operator==(const iterator_base& other) const noexcept {
            return _key_or_tree == other._key_or_tree;
        }
        bool operator!=(const iterator_base& other) const noexcept { return !operator==(other); }
    };

    using iterator_base_const = iterator_base<true>;
    using iterator_base_nonconst = iterator_base<false>;

    class const_iterator : public iterator_base_const {
    public:
        explicit const_iterator(const member_hook::child_pointer_t& bp) noexcept : iterator_base_const(bp) {}
        explicit const_iterator(const member_hook& key) noexcept : iterator_base_const(key) {}
        explicit const_iterator(const tree_base& tree) noexcept : iterator_base_const(tree) {}
        const_iterator() noexcept : iterator_base_const() {}
        const_iterator(const iterator_base_const& o) noexcept : iterator_base_const(o) {}
        const_iterator(const iterator_base_nonconst& o) noexcept : iterator_base_const(o) {}
    };

    class iterator : public iterator_base_nonconst {
        member_hook& this_hook() const noexcept {
            return *iterator_base_nonconst::_key_or_tree.template as<member_hook>();
        }

    public:
        explicit iterator(member_hook& key) noexcept : iterator_base_nonconst(key) {}
        explicit iterator(tree_base& base) noexcept : iterator_base_nonconst(base) {}
        iterator() noexcept : iterator_base_nonconst() {}
        iterator(const iterator_base_const& o) noexcept : iterator_base_nonconst(o) {}
        iterator(const iterator_base_nonconst& o) noexcept : iterator_base_nonconst(o) {}

        // Special 'self'-iterator
        explicit iterator(Key* k) noexcept : iterator(k->*Link) {}

        template <typename Disp>
        requires Disposer<Disp, Key>
        iterator erase_and_dispose(Disp&& disp) noexcept {
            member_hook& cur = this_hook();
            member_hook* next = cur.step<Right>();
            iterator ret = (next != nullptr ? iterator(*next) : iterator(*cur.corner_tree<Right>()));
            cur.erase(next);
            disp(&to_key(&cur));
            return ret;
        }

        iterator erase() noexcept {
            return erase_and_dispose(default_dispose<Key>);
        }

        tree* tree_if_singular() const noexcept {
            member_hook& cur = this_hook();

            if (cur.is_root() && cur._child_or_tree[Left].is<tree_base>() && cur._child_or_tree[Right].is<tree_base>()) {
                tree_base* t = cur.root_tree();
                return boost::intrusive::get_parent_from_member(t, &tree::_base);
            }

            return nullptr;
        }
    };

    const_iterator cbegin() const noexcept {
        return empty() ? cend() : const_iterator(*_base._corners[Left]);
    }
    const_iterator cend() const noexcept {
        return const_iterator(_base);
    }

    iterator begin() noexcept { return iterator(const_this()->cbegin()); }
    iterator end() noexcept { return iterator(const_this()->cend()); }
    const_iterator begin() const noexcept { return cbegin(); }
    const_iterator end() const noexcept { return cend(); }

    using reverse_iterator = std::reverse_iterator<iterator>;
    reverse_iterator rbegin() noexcept { return std::make_reverse_iterator(end()); }
    reverse_iterator rend() noexcept { return std::make_reverse_iterator(begin()); }

    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    const_reverse_iterator crbegin() const noexcept { return std::make_reverse_iterator(cend()); }
    const_reverse_iterator rbegin() const noexcept { return crbegin(); }
    const_reverse_iterator crend() const noexcept { return std::make_reverse_iterator(cbegin()); }
    const_reverse_iterator rend() const noexcept { return crend(); }
};

} // namespace
