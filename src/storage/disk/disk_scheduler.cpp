//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
  //     "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();//阻塞主线程，直至子线程结束
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  auto cur = request_queue_.Get();//如果queue是空，会阻塞，理论上一定能拿到值
  while(cur != std::nullopt){//signal to exit the loop
  if(cur->is_write_){
    disk_manager_->WritePage(cur->page_id_, cur->data_);
  }
  else{
    disk_manager_->ReadPage(cur->page_id_, cur->data_);
  }
  cur->callback_.set_value(true);
  cur = request_queue_.Get();
  }
}

}  // namespace bustub
