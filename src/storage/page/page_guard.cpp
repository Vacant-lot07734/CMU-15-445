#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // noexcept告知编译器该函数不会抛出异常，移动构造函数和移动赋值运算符默认是 noexcept 的
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(this->PageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // Similar to a move constructor, except that the move
  // assignment assumes that BasicPageGuard already has a page
  //  being guarded. Think carefully about what should happen when
  //  a guard replaces its held page with a different one, given
  //  the purpose of a page guard.
  this->Drop();  //不再管理自己的页
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  Drop();
};  // NOLINT告编译器(如GCC和Clang)或静态分析工具(clang-tidy)忽略对特定代码行或代码块的警告。

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  this->page_->RLatch();  //获取读锁，区别写锁，读锁是共享锁
  ReadPageGuard read_guard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();  //获取写锁
  WritePageGuard write_guard(this->bpm_, this->page_);
  // this->page_->WUnlatch();
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); };

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();  //不再管理自己的页
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  // 判断一下防止drop两次
  if (this->guard_.page_ != nullptr && this->guard_.page_ != nullptr) {
    this->guard_.bpm_->UnpinPage(this->guard_.PageId(), this->guard_.is_dirty_);
    this->guard_.page_->RUnlatch();
  }
  this->guard_.bpm_ = nullptr;
  this->guard_.page_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();  //不再管理自己的页
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.bpm_ != nullptr && this->guard_.page_ != nullptr) {
    this->guard_.bpm_->UnpinPage(this->guard_.PageId(), this->guard_.is_dirty_);
    this->guard_.page_->WUnlatch();
  }
  this->guard_.bpm_ = nullptr;
  this->guard_.page_ = nullptr;
  this->guard_.is_dirty_ = true;
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
