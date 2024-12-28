#include "concurrent_kvstore.hpp"

#include <mutex>
#include <optional>

bool ConcurrentKvStore::Get(const GetRequest* req, GetResponse* res) {

  ssize_t bucket = store.bucket(req->key);
  store.mutexes[bucket].lock_shared();
  std::optional<DbItem> item = store.getIfExists(bucket, req->key);
  if (item) {
    res->value = (*item).value;
    store.mutexes[bucket].unlock_shared();
    return true;
  }
  store.mutexes[bucket].unlock_shared();
  return false;
}

bool ConcurrentKvStore::Put(const PutRequest* req, PutResponse*) {

  ssize_t bucket = store.bucket(req->key);
  store.mutexes[bucket].lock();
  store.insertItem(bucket, req->key, req->value);
  store.mutexes[bucket].unlock();
  return true;
}

bool ConcurrentKvStore::Append(const AppendRequest* req, AppendResponse*) {

  ssize_t bucket = store.bucket(req->key);
  store.mutexes[bucket].lock();
  std::optional<DbItem> item = store.getIfExists(bucket, req->key);
  if (item) {
    std::string curr_val = (*item).value;
    curr_val.append(req->value);
    store.insertItem(bucket, req->key, curr_val);
  } else {
    store.insertItem(bucket, req->key, req->value);
  }
  store.mutexes[bucket].unlock();
  return true;
}

bool ConcurrentKvStore::Delete(const DeleteRequest* req, DeleteResponse* res) {

  ssize_t bucket = store.bucket(req->key);
  store.mutexes[bucket].lock();
  std::optional<DbItem> item = store.getIfExists(bucket, req->key);
  if (item) {
    res->value = (*item).value;
    bool ret = store.removeItem(bucket, req->key);
    store.mutexes[bucket].unlock();
    return ret;
  } else {
    store.mutexes[bucket].unlock();
    return false;
  }
}

bool ConcurrentKvStore::MultiGet(const MultiGetRequest* req,
                                 MultiGetResponse* res) {

  for (int i=0; i < (int) req->keys.size(); i++) {
    ssize_t bucket = store.bucket(req->keys[i]);
    store.mutexes[bucket].lock();
    std::optional<DbItem> item = store.getIfExists(bucket, req->keys[i]);
    if (item) {
      res->values.push_back((*item).value);
      store.mutexes[bucket].unlock();
    } else {
      store.mutexes[bucket].unlock();
      return false;
    }
  }
  return true;
}

bool ConcurrentKvStore::MultiPut(const MultiPutRequest* req,
                                 MultiPutResponse*) {

  if (req->keys.size() != req->values.size()) {
    return false;
  }
  for (int i=0; i < (int) req->keys.size(); i++) {
    ssize_t bucket = store.bucket(req->keys[i]);
    store.mutexes[bucket].lock();
    store.insertItem(bucket, req->keys[i], req->values[i]);
    store.mutexes[bucket].unlock();
  }
  return true;
}

std::vector<std::string> ConcurrentKvStore::AllKeys() {

  std::vector<std::string> keys;

  for (int i = 0; i < (int) DbMap::BUCKET_COUNT; i++) {
    store.mutexes[i].lock();
  }
  for (int i = 0; i < (int) DbMap::BUCKET_COUNT; i++) {
    std::list<DbItem> curr_list = store.buckets[i];
    //store.mutexes[i].lock();
    for (std::list<DbItem>::iterator it = curr_list.begin(); it != curr_list.end(); it++) {
      keys.push_back((*it).key);
    }
    store.mutexes[i].unlock();
  }
  return keys;
}
