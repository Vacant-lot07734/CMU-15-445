//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  this->index_name_ = name;
  // header_page_id_ = INVALID_PAGE_ID; 声明时已经开辟空间，NewPage分配新页面
  auto header_guard = bpm->NewPageGuarded(&header_page_id_);
  // AsMut可以修改
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = this->Hash(key);
  // header -> directory As只读
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto directory_index = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();
  // directory -> bucket
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (static_cast<int>(bucket_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  directory_guard.Drop();
  // bucket -> key-value
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;  // bucket_guard 析构函数会自动调用Drop()
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> result;
  bool is_exist = this->GetValue(key, &result, transaction);
  if (is_exist) {
    // 是否是更新 ？？？
    return false;
  }
  auto hash = this->Hash(key);
  // header -> directory
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_index = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    // 添加在新的directory_page
    return InsertToNewDirectory(header_page, directory_index, hash, key, value);
  }
  header_guard.Drop();

  // directory -> bucket
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (static_cast<int>(bucket_page_id) == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_index, key, value);
  }
  // fix bug 不能提前释放directory_page的锁
  // directory_guard.Drop();

  // bucket -> key-value
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->Insert(key, value, cmp_)) {
    // LOG_DEBUG("key INSERT bucket %d Success", bucket_page_id);
    return true;
  }
  // 上面insert失败，说明当前bucket满了

  // 尝试增加ld，分裂bucket
  if (directory_page->GetLocalDepth(bucket_index) == directory_page->GetGlobalDepth()) {
    // gd已到最大，不能增加
    if (directory_page->GetGlobalDepth() >= directory_page->GetMaxDepth()) {
      return false;
    }
    // 增加gd，函数内完成 目录、ld的拷贝，之后才能增加ld
    directory_page->IncrGlobalDepth();
    if (directory_page->GetLocalDepth(bucket_index) == directory_page->GetGlobalDepth()) {
      return false;
    }
  }
  // 需要更新 directory 和新分裂 bucket 的映射 DirectoryMapping
  page_id_t new_bucket_page_id;
  auto tmp_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_guard = tmp_guard.UpgradeWrite();
  // get new_bucket_idx
  auto new_bucket_idx = directory_page->GetSplitImageIndex(bucket_index);
  auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);
  directory_page->IncrLocalDepth(bucket_index);
  // LOG_DEBUG("bucket_idx:%d new_bucket_idx:%d gd:%d", bucket_index, new_bucket_idx, directory_page->GetGlobalDepth());
  UpdateDirectoryMapping(directory_page, new_bucket_idx, new_bucket_page_id,
                         directory_page->GetLocalDepth(bucket_index), directory_page->GetLocalDepthMask(bucket_index));
  // 拆分 bucket 将原来一个 bucket 的 entries 分配到两个
  std::vector<int> remove_kv;
  for (uint32_t i = 0; i < bucket_page->Size(); i++) {
    auto k = bucket_page->KeyAt(i);
    auto v = bucket_page->ValueAt(i);
    auto rehash_idx = directory_page->HashToBucketIndex(Hash(k));
    page_id_t rehash_page_id = directory_page->GetBucketPageId(rehash_idx);
    if (rehash_page_id == new_bucket_page_id) {
      remove_kv.push_back(i);
      new_bucket_page->Insert(k, v, cmp_);
    }
  }
  for (int i = remove_kv.size() - 1; i >= 0; i--) {
    bucket_page->RemoveAt(remove_kv[i]);
  }

  // bool insert_success = InsertToNewDirectory(header_page, directory_index, hash, key, value);
  // 判断插入哪个bucket
  bool insert_success = false;
  auto rehash_bucket_idx = directory_page->HashToBucketIndex(hash);
  if (rehash_bucket_idx == bucket_index) {
    insert_success = bucket_page->Insert(key, value, cmp_);
    // LOG_DEBUG("INSERT bucket %d Success", bucket_page_id);
  } else {
    insert_success = new_bucket_page->Insert(key, value, cmp_);
    // LOG_DEBUG("INSERT bucket %d Success", new_bucket_page_id);
  }

  new_bucket_guard.Drop();
  bucket_guard.Drop();
  directory_guard.Drop();

  return insert_success;
}

/*********************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // 这里的page_id怎么确定？？？
  page_id_t directory_page_id = INVALID_PAGE_ID;
  // NewPageGuard分配新页面，并返回PageGuard
  auto tmp_directory_guard = bpm_->NewPageGuarded(&directory_page_id);
  auto directory_guard = tmp_directory_guard.UpgradeWrite();
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  return InsertToNewBucket(directory_page, bucket_idx, key, value);
}

/*********************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // 这里的page_id怎么确定？？？
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto tmp_bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_guard = tmp_bucket_guard.UpgradeWrite();
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  directory->SetLocalDepth(bucket_idx, 0);
  // LOG_DEBUG("INSERT bucket %d Success", bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

/*********************************************/
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  // directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = this->Hash(key);
  // header -> directory
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  header_guard.Drop();
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  // directory -> bucket
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (static_cast<int>(bucket_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  // bucket -> key-value
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool remove_success = bucket_page->Remove(key, cmp_);
  bucket_guard.Drop();
  if (!remove_success) {
    return false;
  }
  // 可能合并缩容
  // To keep things relatively simple, we provide the following rules for merging:
  // 1. Only empty buckets can be merged.
  // 2. Buckets can only be merged with their split image if their split image has the same local depth.
  // 3. You should keep merging recursively if the new split image of the merged bucket is empty.
  page_id_t bucket1_page_id = bucket_page_id;
  uint32_t bucket1_idx = bucket_idx;
  auto bucket1_page_guard = bpm_->FetchPageRead(bucket1_page_id);
  auto bucket1_page = bucket1_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  auto ld = directory_page->GetLocalDepth(bucket1_idx);
  while (ld > 0) {
    auto bucket2_idx = directory_page->GetSplitImageIndex(bucket1_idx);
    page_id_t bucket2_page_id = directory_page->GetBucketPageId(bucket2_idx);
    auto bucket2_page_guard = bpm_->FetchPageRead(bucket2_page_id);
    auto bucket2_page = bucket2_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (bucket1_page_id == bucket2_page_id) {
      // 指向相同page，没法合并缩容
      break;
    }
    if (ld != directory_page->GetLocalDepth(bucket2_idx) || (!bucket1_page->IsEmpty() && !bucket2_page->IsEmpty())) {
      break;
    }
    if (bucket1_page->IsEmpty()) {
      // 留下bucket2_page
      directory_page->DecrLocalDepth(bucket2_idx);
      UpdateDirectoryMapping(directory_page, bucket1_idx, bucket2_page_id, directory_page->GetLocalDepth(bucket2_idx),
                             directory_page->GetLocalDepthMask(bucket2_idx));
      // 修改，继续循环
      bpm_->DeletePage(bucket1_page_id);
      bucket1_page = bucket2_page;
      bucket1_page_id = bucket2_page_id;
      bucket1_page_guard = std::move(bucket2_page_guard);
    } else {
      directory_page->DecrLocalDepth(bucket1_idx);
      UpdateDirectoryMapping(directory_page, bucket2_idx, bucket1_page_id, directory_page->GetLocalDepth(bucket1_idx),
                             directory_page->GetLocalDepthMask(bucket1_idx));
      // 修改，继续循环
      bpm_->DeletePage(bucket2_page_id);
    }
    ld--;
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
