// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <vineyard/client/client.h>

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/common.pb.h"

#include "ray/object_manager/plasma/client.h"

#include "absl/container/flat_hash_map.h"

namespace plasma {


class VineyardClientImpl : public IPlasmaClientImpl, 
                           public std::enable_shared_from_this<VineyardClientImpl> {
 public:
  VineyardClientImpl();
  ~VineyardClientImpl();

 // IPlasmaClientImpl method implementations

  Status Connect(const std::string &store_socket_name,
                 const std::string &manager_socket_name, int release_delay = 0,
                 int num_retries = -1) override;

  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address, int64_t data_size,
                                const uint8_t *metadata, int64_t metadata_size,
                                std::shared_ptr<Buffer> *data, plasma::flatbuf::ObjectSource source,
                                int device_num = 0) override;

  Status TryCreateImmediately(const ObjectID &object_id,
                              const ray::rpc::Address &owner_address, int64_t data_size,
                              const uint8_t *metadata, int64_t metadata_size,
                              std::shared_ptr<Buffer> *data, plasma::flatbuf::ObjectSource source,
                              int device_num) override;

  Status Get(const std::vector<ObjectID> &object_ids, int64_t timeout_ms,
             std::vector<ObjectBuffer> *object_buffers, bool is_from_worker) override;

  Status Release(const ObjectID &object_id) override;

  Status Contains(const ObjectID &object_id, bool *has_object) override;

  Status Abort(const ObjectID &object_id) override;

  Status Seal(const ObjectID &object_id) override;

  Status Delete(const std::vector<ObjectID> &object_ids) override;

  Status Evict(int64_t num_bytes, int64_t &num_bytes_evicted) override;

  Status Disconnect() override;

  std::string DebugString() override;

  bool IsInUse(const ObjectID &object_id) override;

  int64_t store_capacity() override { return store_capacity_; }
 private:
  int64_t store_capacity_;
  vineyard::Client client_;
};

}  // namespace plasma
