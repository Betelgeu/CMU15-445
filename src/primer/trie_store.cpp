#include "primer/trie_store.h"
#include <optional>
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // throw NotImplementedException("TrieStore::Get is not implemented.");
  // 1. 拿root, 拿完马上释放
  root_lock_.lock();
  Trie root = root_;
  root_lock_.unlock();
  // 2. 获取value
  auto value = root.Get<T>(key);
  // 3. 返回ValueGuard
  if (value == nullptr) {
    return std::nullopt;
  }
  return ValueGuard<T>(root, *value);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Put is not implemented.");
  // 1. 保证只有一个writer, 注意这里的value禁止复制
  write_lock_.lock();
  Trie new_root = root_.Put<T>(key, std::move(value));
  root_lock_.lock();
  root_ = new_root;

  root_lock_.unlock();
  write_lock_.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Remove is not implemented.");
  // 和Put只能说是完全一致
  write_lock_.lock();
  Trie new_root = root_.Remove(key);
  root_lock_.lock();
  root_ = new_root;

  root_lock_.unlock();
  write_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
