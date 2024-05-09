#include "primer/trie.h"
#include <iostream>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // 默认抛出一个未实现的异常
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  // 1. 遍历trie，找到对应的节点
  if (root_ == nullptr) {
    return nullptr;
  }
  const TrieNode *cur = root_.get();
  for (auto x : key) {
    auto it = cur->children_.find(x);
    if (it == cur->children_.end()) {
      return nullptr;
    }
    cur = it->second.get();
  }
  // 2. 使用dynamic_cast转换为TrieNodeWithValue<T>*
  if (cur->is_value_node_) {
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur);
    if (value_node == nullptr) {
      return nullptr;
    }
    return value_node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  if (key.empty()) {
    Trie new_root(std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value))));
    return new_root;
  }

  auto origin_ptr = root_;
  auto cur_ptr = (root_) ? std::shared_ptr<TrieNode>(root_->Clone()) : std::make_shared<TrieNode>();
  Trie new_root(cur_ptr);

  for (std::string_view::size_type i = 0; i + 1 < key.size(); i++) {
    char x = key[i];
    auto it = cur_ptr->children_.find(x);
    std::shared_ptr<TrieNode> next;
    if (it == cur_ptr->children_.end()) {
      next = std::make_shared<TrieNode>();
    } else {
      next = std::shared_ptr<TrieNode>(it->second->Clone());
    }
    cur_ptr->children_[x] = next;
    cur_ptr = next;
  }

  char x = key.back();
  auto it = cur_ptr->children_.find(x);
  if (it == cur_ptr->children_.end()) {
    cur_ptr->children_[x] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    cur_ptr->children_[x] =
        std::make_shared<TrieNodeWithValue<T>>(cur_ptr->children_[x]->children_, std::make_shared<T>(std::move(value)));
  }

  return new_root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  // 空树怎么删都还是空树
  if (root_ == nullptr) {
    return *this;
  }
  // 空key改root
  if (key.empty()) {
    return Trie(std::make_shared<TrieNode>(root_->children_));
  }

  // 找到key对应的节点, 和put类似，将最后的节点修改为TrieNode
  auto origin_ptr = root_;
  auto cur_ptr = std::shared_ptr<TrieNode>(root_->Clone());
  Trie new_root(cur_ptr);
  std::shared_ptr<TrieNode> last_end = nullptr;
  char last_x;
  for (std::string_view::size_type i = 0; i + 1 < key.size(); i++) {
    if (cur_ptr->children_.size() > 1 || cur_ptr->is_value_node_) {
      last_end = cur_ptr;
      last_x = key[i];
    }

    char x = key[i];
    auto it = cur_ptr->children_.find(x);
    std::shared_ptr<TrieNode> next;
    if (it == cur_ptr->children_.end()) {
      next = std::make_shared<TrieNode>();
    } else {
      next = std::shared_ptr<TrieNode>(it->second->Clone());
    }
    cur_ptr->children_[x] = next;
    cur_ptr = next;
  }
  if (cur_ptr->children_.size() > 1 || cur_ptr->is_value_node_) {
    last_end = cur_ptr;
    last_x = key.back();
  }

  char x = key.back();
  auto it = cur_ptr->children_.find(x);
  if (it == cur_ptr->children_.end()) {
    return *this;
  }
  // found node: cur_ptr->children_[x]
  if (cur_ptr->children_[x]->children_.empty()) {
    if (last_end) {
      last_end->children_.erase(last_x);
    } else {
      return {};
    }
  } else {
    cur_ptr->children_[x] = std::make_shared<TrieNode>(cur_ptr->children_[x]->children_);
  }
  return new_root;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
