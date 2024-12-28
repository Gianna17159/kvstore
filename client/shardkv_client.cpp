#include "shardkv_client.hpp"

std::optional<std::string> ShardKvClient::Get(const std::string& key) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return std::nullopt;

  // find responsible server in config
  std::optional<std::string> server = config->get_server(key);
  if (!server) return std::nullopt;

  return SimpleClient{*server}.Get(key);
}

bool ShardKvClient::Put(const std::string& key, const std::string& value) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return false;

  // find responsible server in config, then make Put request
  std::optional<std::string> server = config->get_server(key);
  if (!server) return false;
  return SimpleClient{*server}.Put(key, value);
}

bool ShardKvClient::Append(const std::string& key, const std::string& value) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return false;

  // find responsible server in config, then make Append request
  std::optional<std::string> server = config->get_server(key);
  if (!server) return false;
  return SimpleClient{*server}.Append(key, value);
}

std::optional<std::string> ShardKvClient::Delete(const std::string& key) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return std::nullopt;

  // find responsible server in config, then make Delete request
  std::optional<std::string> server = config->get_server(key);
  if (!server) return std::nullopt;
  return SimpleClient{*server}.Delete(key);
}

std::optional<std::vector<std::string>> ShardKvClient::MultiGet(
    const std::vector<std::string>& keys) {
  std::vector<std::string> values;
  values.reserve(keys.size());
  // TODO (Part B, Step 3): Implement!

  auto config = this->Query();
  if (!config) {
    return std::nullopt;
  }
  std::map<std::string, std::vector<std::string>> server_to_keys;
  std::map<std::string, std::string> keys_to_values;
  for (std::string key : keys) {
    std::optional<std::string> server = config->get_server(key);
    if (!server) {
      return std::nullopt;
    }
    server_to_keys[*server].push_back(key);
  }
  for (auto&& [server, responsible] : server_to_keys) {
    std::optional<std::vector<std::string>> curr_serv_values = SimpleClient{server}.MultiGet(responsible);
    if (!curr_serv_values) {
      return std::nullopt;
    }
    for (int i=0; i < (int) responsible.size(); i++) {
      keys_to_values[responsible[i]] = (*curr_serv_values)[i];
    }
  }
  for (std::string key : keys) {
    values.push_back(keys_to_values[key]);
  }
  return values;
}

bool ShardKvClient::MultiPut(const std::vector<std::string>& keys,
                             const std::vector<std::string>& values) {
  // TODO (Part B, Step 3): Implement!
   auto config = this->Query();
  if (!config) {
    return false;
  }
  std::map<std::string, std::vector<std::string>> server_to_keys;
  std::map<std::string, std::vector<std::string>> server_to_values;
  for (int i=0; i < (int) keys.size(); i++) {
    std::optional<std::string> server = config->get_server(keys[i]);
    if (!server) {
      return false;
    }
    server_to_keys[*server].push_back(keys[i]);
    server_to_values[*server].push_back(values[i]);
  }
  for (auto&& [server, responsible] : server_to_keys) {
    bool ret = SimpleClient{server}.MultiPut(responsible, server_to_values[server]);
    if (!ret) {
      return false;
    }
  }
  return true;
}

// Shardcontroller functions
std::optional<ShardControllerConfig> ShardKvClient::Query() {
  QueryRequest req;
  if (!this->shardcontroller_conn->send_request(req)) return std::nullopt;

  std::optional<Response> res = this->shardcontroller_conn->recv_response();
  if (!res) return std::nullopt;
  if (auto* query_res = std::get_if<QueryResponse>(&*res)) {
    return query_res->config;
  }

  return std::nullopt;
}

bool ShardKvClient::Move(const std::string& dest_server,
                         const std::vector<Shard>& shards) {
  MoveRequest req{dest_server, shards};
  if (!this->shardcontroller_conn->send_request(req)) return false;

  std::optional<Response> res = this->shardcontroller_conn->recv_response();
  if (!res) return false;
  if (auto* move_res = std::get_if<MoveResponse>(&*res)) {
    return true;
  }

  return false;
}
