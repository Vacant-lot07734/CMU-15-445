#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <stack>
#include <string_view>
// #include "iostream"
// #include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  //返回的是value的指针！！！
  auto node = this->root_;
  if(!node || (key.empty()&&!node->is_value_node_)){
    return nullptr;
  }
  for(auto &c : key){
    auto cur = node->children_.find(c);//暂时忽略node为空的情况
    if(cur == node->children_.end()){
      return nullptr;
    }
    node = cur->second;//考虑使用std::const_pointer_cast
  }
  if(!node || !node->is_value_node_){
    return nullptr;
  }
  if(auto res = dynamic_cast<const TrieNodeWithValue<T> *>(node.get()); res){
    return res->value_.get();
  }else{
    return nullptr;
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  
  // 父节点可以有值.
  //暂时认为有key为空，value不为空的情况
  //忽略value为空的情况
  auto tmp = this->root_ ? root_->Clone() : std::make_unique<TrieNode>();
  std::shared_ptr<TrieNode> new_root = std::move(tmp);
  //工作指针
  auto node = new_root;
  std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
  auto n = key.size();
  if(key.empty()){
    //创建带值结点
    auto value_node = std::make_shared<TrieNodeWithValue<T>>(node->children_, value_ptr);
    new_root = std::move(value_node);
    return Trie{new_root};
  }else{  
    for(size_t i = 0; i < n - 1; i++){
      if(auto temp = node->children_.find(key[i]); temp != node->children_.end()){
        node->children_[key[i]] = std::shared_ptr<const TrieNode>(temp->second->Clone());//可能是带值结点，clone会调用派生类方法
      }else{
        node->children_[key[i]] = std::make_shared<const TrieNode>();
      }
      node = std::const_pointer_cast<TrieNode>(node->children_[key[i]]);//疑问，如果是带值结点呢,要用const_pointer_cast而不是dynamic
      // node = node->children_[key[i]];//转换错误
    }
  }
  if(auto leaf = node->children_.find(key[n - 1]); leaf != node->children_.end()){
    node->children_[key[n - 1]] = std::make_shared<TrieNodeWithValue<T>>(node->children_[key[n - 1]]->children_, std::move(value_ptr));
  }else{
    node->children_[key[n - 1]] = std::make_shared<TrieNodeWithValue<T>>(std::move(value_ptr));
  }
  return Trie{new_root};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  //工作指针
  auto node = this->root_;
  std::stack<std::shared_ptr<const TrieNode>> st;
  for(auto ch : key){
    if(auto temp = node->children_.find(ch); temp != node->children_.end()){
      // node->children_[key[ch]] = std::shared_ptr<const TrieNode>(temp->second->Clone());
      st.push(node);
      // node = std::dynamic_pointer_cast<TrieNode>(node->children_[key[ch]]);
      node = std::dynamic_pointer_cast<const TrieNode>(temp->second);
    }
    else{
      return Trie(root_);
    }
  }
  if(!node->is_value_node_){
    return Trie(node);
  }else{
    // std::cout<<"改为非值结点"<<" ";
    //删除逻辑，值结点改为非值结点
    node = std::make_shared<const TrieNode>(node->children_);
  }
  //当前栈顶是父节点，node是子节点
  for(auto idx = key.rbegin(); idx != key.rend(); idx++){
    auto fa = st.top()->Clone();
    st.pop();
    if(node->children_.empty() && !node->is_value_node_){
      //删除逻辑，无孩子，非值结点
      fa->children_.erase(*idx);
      // std::cout<<"删除一个"<<" ";
    }
    else{
      fa->children_[*idx] = node;
    }
    //转移所有权,node = std::move(fa);应该也可以
    node = std::shared_ptr<const TrieNode>(std::move(fa));
  }
  return node->children_.empty() ? Trie() : Trie(node);

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
