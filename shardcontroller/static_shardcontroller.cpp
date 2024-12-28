#include "static_shardcontroller.hpp"

bool StaticShardController::Query(const QueryRequest*, QueryResponse* res) {
  
  config_mtx.lock();
  res->config = config;
  config_mtx.unlock();
  return true;
}

bool StaticShardController::Join(const JoinRequest* req, JoinResponse*) {
  
  config_mtx.lock();
  if (config.server_to_shards.contains(req->server)) {
    config_mtx.unlock();
    return false;
  }
  std::vector<Shard> shards;
  config.server_to_shards.insert({req->server, shards});
  config_mtx.unlock();
  cout_color(BLUE, "Added server ", req->server,
             " to shardcontroller configuration.");
  return true;
}

bool StaticShardController::Leave(const LeaveRequest* req, LeaveResponse*) {
  
  config_mtx.lock();
  if (!(config.server_to_shards.contains(req->server))) {
    config_mtx.unlock();
    return false;
  }
  std::vector<Shard> del_shards = config.server_to_shards[req->server];
  config.server_to_shards.erase(req->server);

  cout_color(BLUE, "Deleted server ", req->server,
             " on shardcontroller configuration.");

  if (config.server_to_shards.empty()) {
    config_mtx.unlock();
    return true;
  }

  std::map<std::string, std::vector<Shard>>::iterator first_el = config.server_to_shards.begin();
  
  for(int i = 0; i < (int) del_shards.size(); i++) {
    first_el->second.push_back(del_shards[i]);
  }
  config_mtx.unlock();
  return true;
}

bool StaticShardController::Move(const MoveRequest* req, MoveResponse*) {
  
  config_mtx.lock();
  if (!(config.server_to_shards.contains(req->server))) {
    config_mtx.unlock();
    return false;
  }
  // For each shard to be moved, iterate over each server's shards.
  // For each of the server's shards, if 'moved' overlaps with 'shard', compute
  // the modified shard and insert it into 'new_shards.' Once the loop ends, we
  // replace the server's shards with 'new_shards.' 
  for (Shard moved : req->shards) {
    for (auto&& [server, shards] : this->config.server_to_shards) {
      std::vector<Shard> new_shards;
      for (Shard shard : shards) {
        // If the moved shard doesn't have the same granularity as the current
        // shard, emit an error and return
        if (moved.granularity() != shard.granularity()) {
          cerr_color(
              RED,
              "Moving differing shard granularities not currently supported.");
          return false;
        }
        // Using overlap status, determine whether shards need to be modified
        OverlapStatus os = get_overlap(shard, moved);
        switch (os) {
          case OverlapStatus::NO_OVERLAP: {
           
            new_shards.push_back(shard);
            continue;
          }
          case OverlapStatus::OVERLAP_START: {
            
            std::pair<Shard, Shard> split_pair = split_shard(shard, moved.upper, true);
            new_shards.push_back(split_pair.second);
            continue;
          }
          case OverlapStatus::OVERLAP_END: {
            
            std::pair<Shard, Shard> split_pair = split_shard(shard, moved.lower, false);
            new_shards.push_back(split_pair.first);
            continue;
          }
          case OverlapStatus::COMPLETELY_CONTAINS: {
            
            std::pair<Shard, Shard> split_pair1 = split_shard(shard, moved.lower, false);
            new_shards.push_back(split_pair1.first);

            std::pair<Shard, Shard> split_pair2 = split_shard(shard, moved.upper, true);
            new_shards.push_back(split_pair2.second);
            continue;
          }
          case OverlapStatus::COMPLETELY_CONTAINED:
            continue;
        }
      }
      shards = new_shards;
    }
  }
  for (Shard move : req->shards) {
    config.server_to_shards[req->server].push_back(move);
  }
  //move the shard onto the target server
  
  cout_color(DIM, "Moved the following shards to server ", req->server, ":");
  for (auto&& s : req->shards) print_color(std::cout, DIM, s, " ");
  std::cout << '\n';
  config_mtx.unlock();
  return true;
}

/* ==================================================*/
/* === INTERNALS: DO NOT MODIFY BELOW THIS LINE ===  */
/* ==================================================*/

int StaticShardController::start() {
  this->is_stopped = false;

  // Create listener socket, and start client listener
  this->listener_fd = open_listener_socket(address);
  if (this->listener_fd < 0) {
    return -1;
  }
  this->client_listener =
      std::thread(&StaticShardController::accept_clients_loop, this);

  cout_color(BLUE, "Listening on ", this->address);
  return 0;
}

void StaticShardController::stop() {
  this->is_stopped = true;

  // Shutdown listener, and stop accepting clients
  shutdown(this->listener_fd, SHUT_RDWR);
  cout_color(BLUE, "Joining listener thread...");
  this->client_listener.join();

  // Close all connections
  cout_color(BLUE, "Closing all connections...");
  std::unique_lock lock(this->conns_mtx);
  for (auto&& c : this->current_conns) {
    cout_color(BLUE, "Closing connection from ", c->address);
    c->shutdown();
  }
  // Wait for all connections to close (since we've detached threads)
  conns_cv.wait(lock, [this] { return this->current_conns.empty(); });

  // ... and we're done!
}

void StaticShardController::accept_clients_loop() {
  while (!this->is_stopped) {
    std::shared_ptr<ClientConn> conn = accept_client(this->listener_fd);
    if (!conn) {
      return;
    }

    // NOTE: for now, let's just spawn a thread to handle each client for
    // simplicity.
    std::unique_lock lock(this->conns_mtx);
    std::thread conn_thread(&StaticShardController::handle_client, this, conn);
    conn_thread.detach();
    this->current_conns.push_back(conn);

    cout_color(BLUE, "Shardcontroller received client connection from ",
               conn->address, " on socket ", conn->fd);
  }
}

void StaticShardController::handle_client(std::shared_ptr<ClientConn> client) {
  while (!is_stopped && client->is_connected) {
    std::optional<Request> req = client->recv_request();
    if (!req) {
      break;
    }

    Response res = this->process_request(*req);
    if (!client->send_response(res)) {
      break;
    }
  }
  // Regardless of whether the shardcontroller is stopped or a message failed to
  // be sent/received, clean up client (should be automatically freed once it
  // goes out of scope)

  client->shutdown();

  // remove from existing connections
  std::unique_lock lock(this->conns_mtx);
  if (auto it = std::find(this->current_conns.begin(),
                          this->current_conns.end(), client);
      it != this->current_conns.end()) {
    this->current_conns.erase(it);
  } else {
    cerr_color(YELLOW,
               "Connection should not already be removed; please post "
               "privately on Edstem if "
               "you receieve this error!");
  }

  // notify waiting cv if last connection
  if (this->current_conns.empty()) {
    this->conns_cv.notify_all();
  }
}

Response StaticShardController::process_request(Request req) {
  Response res;
  if (auto* join_req = std::get_if<JoinRequest>(&req)) {
    JoinResponse join_res{};
    if (this->Join(join_req, &join_res)) {
      res = join_res;
    } else {
      res = ErrorResponse{"Failed to process Join request."};
    }
  } else if (auto* leave_req = std::get_if<LeaveRequest>(&req)) {
    LeaveResponse leave_res{};
    if (this->Leave(leave_req, &leave_res)) {
      res = leave_res;
    } else {
      res = ErrorResponse{"Failed to process Leave request."};
    }
  } else if (auto* move_req = std::get_if<MoveRequest>(&req)) {
    MoveResponse move_res{};
    if (this->Move(move_req, &move_res)) {
      res = move_res;
    } else {
      res = ErrorResponse{"Failed to process Move request."};
    }
  } else if (auto* query_req = std::get_if<QueryRequest>(&req)) {
    QueryResponse query_res{};
    if (this->Query(query_req, &query_res)) {
      res = query_res;
    } else {
      res = ErrorResponse{"Failed to process Query request."};
    }
  } else {
    throw std::logic_error{"invalid request variant!"};
  }
  return res;
}
