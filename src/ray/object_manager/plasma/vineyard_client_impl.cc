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

#include "ray/object_manager/plasma/vineyard_client_impl.h"

#include <cstring>

#include <algorithm>
#include <deque>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <vineyard/client/client.h>
#include <vineyard/client/ds/blob.h>
#include <vineyard/client/ds/object_meta.h>
#include <vineyard/common/util/base64.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"

namespace fb = plasma::flatbuf;

namespace plasma {

using fb::MessageType;
using fb::PlasmaError;

VineyardClientImpl::VineyardClientImpl() : store_capacity_(0), client_() {}

VineyardClientImpl::~VineyardClientImpl() {}

Status VineyardClientImpl::CreateAndSpillIfNeeded(
    const ObjectID &object_id, const ray::rpc::Address &owner_address, int64_t data_size,
    const uint8_t *metadata, int64_t metadata_size, std::shared_ptr<Buffer> *data,
    fb::ObjectSource source, int device_num) {

  std::string bin_oid = object_id.Binary();
  vineyard::ExternalID external_id = vineyard::base64_encode(bin_oid);

  std::unique_ptr<vineyard::BlobWriter> blob;
  VINEYARD_CHECK_OK( client_.CreateBlobExternal(data_size + metadata_size, external_id, metadata_size, blob) );

  *data = std::make_shared<PlasmaMutableBuffer>(shared_from_this(), reinterpret_cast<uint8_t*>(blob->data()), data_size);
  //TODO(mengke): enable spilling

  // The metadata should come right after the data.
  if (metadata != NULL) {
    memcpy((*data)->Data() + data_size, metadata, metadata_size);
  }

  RAY_LOG(INFO) << "VINEYARD: " << "Create a buffer with size: " << data_size << " " << metadata_size;
  return Status::OK();
}

Status VineyardClientImpl::TryCreateImmediately(
    const ObjectID &object_id, const ray::rpc::Address &owner_address, int64_t data_size,
    const uint8_t *metadata, int64_t metadata_size, std::shared_ptr<Buffer> *data,
    fb::ObjectSource source, int device_num) {
  RAY_LOG(INFO) << "VINEYARD: " << "TryCreateImmediately, currently do nothing";
 //TODO(mengke): implement TryCreateImmediately with Create
  return Status::OK();
}


Status VineyardClientImpl::Get(const std::vector<ObjectID> &object_ids,
                               int64_t timeout_ms, std::vector<ObjectBuffer> *out,
                               bool is_from_worker) {
  RAY_LOG(INFO) << "VINEYARD: " << "get objects form vineyard";

  const auto wrap_buffer = [=](const ObjectID &object_id,
                               const std::shared_ptr<Buffer> &buffer) {
    return std::make_shared<PlasmaBuffer>(shared_from_this(), object_id, buffer);
  };
  const int64_t num_objects = object_ids.size();

  std::vector<vineyard::ExternalID> eids;
  for(auto object_id: object_ids) {
    std::string bin_oid = object_id.Binary();
    vineyard::ExternalID external_id = vineyard::base64_encode(bin_oid);
    eids.emplace_back(external_id);
  }

  *out = std::vector<ObjectBuffer>(num_objects);
  std::map<vineyard::ExternalID, vineyard::Payload> payloads;
  std::map<vineyard::ExternalID, std::shared_ptr<arrow::Buffer>> buffers;
  client_.GetBlobsByExternal(std::set<vineyard::ExternalID>(eids.begin(), eids.end()), payloads, buffers);

  ObjectBuffer* object_buffers = &(*out)[0];

  for (int64_t i = 0; i < num_objects; ++i) {

    std::shared_ptr<arrow::Buffer> arrow_buffer = buffers.find(eids[i])->second;
    vineyard::Payload payload = payloads.find(eids[i])->second;
    uint8_t* data =const_cast<uint8_t*>(arrow_buffer->data());
    size_t metadata_size = payload.external_size;
    size_t data_size = arrow_buffer->size() - metadata_size;

    std::shared_ptr<Buffer> physical_buf;
    physical_buf = std::make_shared<SharedMemoryBuffer>(data, data_size + metadata_size);
    physical_buf = wrap_buffer(object_ids[i], physical_buf);

    object_buffers[i].data = SharedMemoryBuffer::Slice(physical_buf, 0, data_size);
    object_buffers[i].metadata = SharedMemoryBuffer::Slice(
          physical_buf, data_size, metadata_size);
    object_buffers[i].device_num = 0;
  }

  return Status::OK();
}

Status VineyardClientImpl::Release(const ObjectID &object_id) {
  RAY_LOG(INFO) << "VINEYARD: " << "Release objects in vineyard";
  std::string bin_oid = object_id.Binary();
  vineyard::ExternalID external_id = vineyard::base64_encode(bin_oid);
  VINEYARD_CHECK_OK(client_.Release(external_id));
  return Status::OK();
}

Status VineyardClientImpl::Contains(const ObjectID &object_id, bool *has_object) {

  RAY_LOG(INFO) << "VINEYARD: " << "check the existence of one given object in vineyard";
  std::string bin_oid = object_id.Binary();
  vineyard::ExternalID external_id = vineyard::base64_encode(bin_oid);
  VINEYARD_CHECK_OK(client_.Exists(external_id));
  return Status::Invalid("Not supported.");
}

Status VineyardClientImpl::Seal(const ObjectID &object_id) {
  RAY_LOG(INFO) << "VINEYARD: " << "Seal the object in vineyard";
  // already sealed in creation.
  return Status::OK();
}

Status VineyardClientImpl::Abort(const ObjectID &object_id) {
  RAY_LOG(INFO) << "VINEYARD: " << "Abort object, currently not supported";
  //TODO(mengke): client_.Abort(Externalid);
  return Status::Invalid("Not supported.");
}

Status VineyardClientImpl::Delete(const std::vector<ObjectID> &object_ids) {
  RAY_LOG(INFO) << "VINEYARD: " << "Delete objects, currently not supported";
  for
  return Status::OK();
}

Status VineyardClientImpl::Evict(int64_t num_bytes, int64_t &num_bytes_evicted) {
  RAY_LOG(INFO) << "VINEYARD: " << "evict some objects, currently not supported";
  //TODO(mengke): implement evict policy in vineyard.
  return Status::OK();
}

Status VineyardClientImpl::Connect(const std::string &store_socket_name,
                                   const std::string &manager_socket_name,
                                   int release_delay, int num_retries) {
  RAY_LOG(INFO) << "VINEYARD: " << "conected to Vineyard : " << store_socket_name;
  const std::string dbg_socket_name = "/var/run/vineyard.sock";
  VINEYARD_CHECK_OK(client_.Connect(dbg_socket_name));
  RAY_LOG(INFO) << "VINEYARD: " << "conected done ";
  return Status::OK();
}

Status VineyardClientImpl::Disconnect() {
  RAY_LOG(INFO) << "VINEYARD: " << "disconnect to vineyard, currently do nothing";
  //TODO(mengke): add disconnect in vineyard
  return Status::OK();
}

bool VineyardClientImpl::IsInUse(const ObjectID &object_id) {
  RAY_LOG(INFO) << "VINEYARD: " << "check object in use, currently do nothing";
  // TODO(mengke): implement the in-use map
  return true;
}

std::string VineyardClientImpl::DebugString() {
  RAY_LOG(INFO) << "VINEYARD: " << "get debugstring from vineyard, currently do nothing.";
  //TODO(mengke): 
  return std::string("No comments");
}

}  // namespace plasma
