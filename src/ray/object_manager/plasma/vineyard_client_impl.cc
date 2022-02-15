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

// TODO
bool VineyardClientImpl::IsInUse(const ObjectID &object_id) {
  RAY_LOG(INFO) << "MENGKE: " << "check object in use, currently do nothing";
  return true;
}

//TODO how to store metadata
Status VineyardClientImpl::CreateAndSpillIfNeeded(
    const ObjectID &object_id, const ray::rpc::Address &owner_address, int64_t data_size,
    const uint8_t *metadata, int64_t metadata_size, std::shared_ptr<Buffer> *data,
    fb::ObjectSource source, int device_num) {

  RAY_LOG(INFO) << "MENGKE: " << "Create a Buffer, currently no spilling";

  std::string bin_oid = object_id.Binary();
  vineyard::ExternalID external_id = vineyard::base64_encode(bin_oid);

  RAY_LOG(INFO) << "MENGKE: " << "CreateBlobWithExternalData " << data_size << ", " << metadata_size;
  RAY_LOG(INFO) << "MENGKE: " << object_id << " in " << external_id;

  std::unique_ptr<vineyard::BlobWriter> blob;
  VINEYARD_CHECK_OK( client_.CreateBlobExternal(data_size + metadata_size, external_id, metadata_size, blob) );

  *data = std::make_shared<PlasmaMutableBuffer>(shared_from_this(), reinterpret_cast<uint8_t*>(blob->data()), data_size);

  // The metadata should come right after the data.
  if (metadata != NULL) {
    memcpy((*data)->Data() + data_size, metadata, metadata_size);
  }

  uint8_t* dbg = (*data)->Data();
  for(auto j = data_size; j < data_size + metadata_size; ++j) {
    RAY_LOG(INFO) << "MENGKEMETA: " << dbg[j];
  }

  RAY_LOG(INFO) << "MENGKE: " << "Create a buffer with size: " << data_size << " " << metadata_size;
  return Status::OK();
}

//TODO
Status VineyardClientImpl::TryCreateImmediately(
    const ObjectID &object_id, const ray::rpc::Address &owner_address, int64_t data_size,
    const uint8_t *metadata, int64_t metadata_size, std::shared_ptr<Buffer> *data,
    fb::ObjectSource source, int device_num) {
  RAY_LOG(INFO) << "MENGKE: " << "TryCreateImmediately, currently do nothing";
  return Status::OK();
}


//TODO
Status VineyardClientImpl::Get(const std::vector<ObjectID> &object_ids,
                               int64_t timeout_ms, std::vector<ObjectBuffer> *out,
                               bool is_from_worker) {
  RAY_LOG(INFO) << "MENGKE: " << "get objects form vineyard";

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
    RAY_LOG(INFO) << "MENGKE: " << "Get object " <<  object_ids[i] << " - " <<  eids[i] << " data_size: " << data_size << " meta_size: " << metadata_size;
    for(size_t j = 0; j < data_size; ++j) {
      RAY_LOG(INFO) << "MENGKEDATA: " <<  data[j];
    }
    for(auto j = data_size; j < data_size + metadata_size; ++j) {
      RAY_LOG(INFO) << "MENGKEMETA: " <<  data[j];
    }

    std::shared_ptr<Buffer> physical_buf;
    physical_buf = std::make_shared<SharedMemoryBuffer>(data, data_size + metadata_size);
    physical_buf = wrap_buffer(object_ids[i], physical_buf);

    object_buffers[i].data = SharedMemoryBuffer::Slice(physical_buf, 0, data_size);
    object_buffers[i].metadata = SharedMemoryBuffer::Slice(
          physical_buf, data_size, metadata_size);
    object_buffers[i].device_num = 0;
  }

  RAY_LOG(INFO) << "MENGKE: " << "Get objects done ";
  return Status::OK();
}

//TODO
Status VineyardClientImpl::Release(const ObjectID &object_id) {
  RAY_LOG(INFO) << "MENGKE: " << "get objects form vineyard";
  return Status::OK();
}

// This method is used to query whether the plasma store contains an object.
// TODO
Status VineyardClientImpl::Contains(const ObjectID &object_id, bool *has_object) {

  RAY_LOG(INFO) << "MENGKE: " << "check the existence of one given object in vineyard, currently not supported, cause panic";
  return Status::Invalid("Not supported.");
}

//TODO
Status VineyardClientImpl::Seal(const ObjectID &object_id) {
  RAY_LOG(INFO) << "MENGKE: " << "Seal the object in vineyard";
  return Status::OK();
}

//TODO
Status VineyardClientImpl::Abort(const ObjectID &object_id) {
  RAY_LOG(INFO) << "MENGKE: " << "Abort object, currently not supported, cause panic";
  return Status::Invalid("Not supported.");
}

//TODO
Status VineyardClientImpl::Delete(const std::vector<ObjectID> &object_ids) {
  RAY_LOG(INFO) << "MENGKE: " << "Delete objects, currently not supported";
  return Status::OK();
}

//TODO
Status VineyardClientImpl::Evict(int64_t num_bytes, int64_t &num_bytes_evicted) {
  RAY_LOG(INFO) << "MENGKE: " << "evict some objects, currently not supported";
  return Status::OK();
}

//TODO remember to stop the plasma server
Status VineyardClientImpl::Connect(const std::string &store_socket_name,
                                   const std::string &manager_socket_name,
                                   int release_delay, int num_retries) {
  RAY_LOG(INFO) << "MENGKE: " << "conected to Vineyard : " << store_socket_name;
  const std::string dbg_socket_name = "/var/run/vineyard.sock";
  VINEYARD_CHECK_OK(client_.Connect(dbg_socket_name));
  RAY_LOG(INFO) << "MENGKE: " << "conected done ";
  return Status::OK();
}

//TODO
Status VineyardClientImpl::Disconnect() {
  RAY_LOG(INFO) << "MENGKE: " << "disconnect to vineyard, currently do nothing";
  return Status::OK();
}

//TODO
std::string VineyardClientImpl::DebugString() {
  RAY_LOG(INFO) << "MENGKE: " << "get debugstring from vineyard, currently do nothing.";
  return std::string("No comments");
}

}  // namespace plasma
