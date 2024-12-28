#include "simple_kvstore.hpp"

bool SimpleKvStore::Get(const GetRequest* req, GetResponse* res) {

  std::string key = req->key;
  map_mutex.lock();
  if (kv_map.find(key) != kv_map.end()) {
    res->value = kv_map[key];
    map_mutex.unlock();
    return true;
  }
  map_mutex.unlock();
  return false;
}

bool SimpleKvStore::Put(const PutRequest* req, PutResponse*) {

  map_mutex.lock();
  kv_map.insert_or_assign(req->key, req->value);
  map_mutex.unlock();
  return true;
}

bool SimpleKvStore::Append(const AppendRequest* req, AppendResponse*) {

  map_mutex.lock();
  if (kv_map.find(req->key) != kv_map.end()) {
    std::string curr_val = kv_map[req->key];
    curr_val.append(req->value);
    kv_map.insert_or_assign(req->key, curr_val);
  } else {
    kv_map.insert({req->key, req->value});
  }
  map_mutex.unlock();
  return true;
}

bool SimpleKvStore::Delete(const DeleteRequest* req, DeleteResponse* res) {

  map_mutex.lock();
  if (kv_map.contains(req->key)) {
    res->value = kv_map[req->key];
    kv_map.erase(req->key);
    map_mutex.unlock();
    return true;
  } else {
    map_mutex.unlock();
    return false;
  }
}

bool SimpleKvStore::MultiGet(const MultiGetRequest* req,
                             MultiGetResponse* res) {

  map_mutex.lock();
  for (int i=0; i < (int) req->keys.size(); i++) {
    if (kv_map.contains(req->keys[i])) {
      res->values.push_back(kv_map[req->keys[i]]);
    } else {
      map_mutex.unlock();
      return false;
    }
  }
  map_mutex.unlock();
  return true;
}

bool SimpleKvStore::MultiPut(const MultiPutRequest* req, MultiPutResponse*) {

  if (req->keys.size() != req->values.size()) {
    return false;
  }
  map_mutex.lock();
  for (int i=0; i < (int) req->keys.size(); i++) {
    kv_map.insert_or_assign(req->keys[i], req->values[i]);
  }
  map_mutex.unlock();
  return true;
}

std::vector<std::string> SimpleKvStore::AllKeys() {

  map_mutex.lock();
  std::vector<std::string> keys;
  for (std::map<std::string, std::string>::iterator it = kv_map.begin(); it != kv_map.end(); it ++) {
    keys.push_back(it->first);
  }
  map_mutex.unlock();
  return keys;
}
