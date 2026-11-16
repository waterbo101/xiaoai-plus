#include "realtime/client.hpp"

#include <algorithm>
#include <chrono>
#include <system_error>
#include <thread>

#include <nlohmann/json.hpp>
#include "common/log.hpp"

namespace xiaoai_plus::realtime {

namespace {

const auto kLog = xiaoai_plus::GetLogger("realtime");

// The server recycles idle realtime sockets roughly every 15 minutes.
// IXWebSocket reconnects transparently, but a reconnect only fires
// onOpen (ws_connected_ = true) and never replays the application
// level StartConnection handshake. Such a socket looks alive to us
// yet the server will not answer StartSession on it. Force a full
// rebuild once a handshaken connection is older than this.
const auto kMaxConnectionAge = std::chrono::minutes(10);

std::string GenSessionId() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return "sid-" + std::to_string(now);
}

bool AsBool(const nlohmann::json& v) {
  return v.is_boolean() && v.get<bool>();
}

std::string AsString(const nlohmann::json& v) {
  return v.is_string() ? v.get<std::string>() : std::string();
}

}  // namespace

Client::Client(config::Config cfg, Callbacks callbacks)
    : cfg_(std::move(cfg)), callbacks_(std::move(callbacks)), rng_(std::random_device{}()) {}

Client::~Client() { Stop(); }

bool Client::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return true;
  }

  sender_thread_ = std::thread([this]() {
    while (running_.load()) {
      std::vector<uint8_t> chunk;
      {
        std::unique_lock<std::mutex> lock(audio_mu_);
        audio_cv_.wait(lock, [this]() { return !running_.load() || !audio_queue_.empty(); });
        if (!running_.load()) {
          break;
        }
        chunk = std::move(audio_queue_.front());
        audio_queue_.pop_front();
      }

      std::string sid;
      {
        std::lock_guard<std::mutex> lock(conn_mu_);
        sid = session_id_;
      }
      if (sid.empty()) {
        continue;
      }

      try {
        auto frame = protocol::BuildAudioFrame(protocol::EventId::kTaskRequest, sid, chunk);
        WriteBinary(frame);
      } catch (...) {
        kLog->warn("audio frame send failed");
      }
    }
  });

  return true;
}

void Client::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  audio_cv_.notify_all();
  control_cv_.notify_all();

  CloseConnection(true);
  if (sender_thread_.joinable()) {
    sender_thread_.join();
  }
}

bool Client::StartSession(std::chrono::milliseconds timeout) {
  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    if (!session_id_.empty()) {
      return true;
    }
  }

  if (!running_.load()) {
    return false;
  }

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  auto remaining_timeout = [&deadline]() {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return std::chrono::milliseconds{0};
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
  };

  nlohmann::json dialog = {
      {"bot_name", cfg_.realtime.preset.bot_name},
      {"extra",
       {{"input_mod", "keep_alive"},
        {"model", cfg_.realtime.preset.model},
        {"enable_music", cfg_.realtime.preset.enable_music}}},
  };
  if (!cfg_.realtime.preset.system_role.empty()) {
    dialog["system_role"] = cfg_.realtime.preset.system_role;
  }
  if (!cfg_.realtime.preset.speaking_style.empty()) {
    dialog["speaking_style"] = cfg_.realtime.preset.speaking_style;
  }

  nlohmann::json payload = {
      {"dialog", std::move(dialog)},
      {"tts", {{"speaker", "zh_female_vv_jupiter_bigtts"},
               {"audio_config", {{"channel", cfg_.audio.channels},
                                    {"format", "pcm_s16le"},
                                    {"sample_rate", cfg_.audio.sample_rate}}}}},
  };

  for (int attempt = 0; attempt < 2 && running_.load(); ++attempt) {
    const auto connect_timeout = remaining_timeout();
    if (connect_timeout.count() <= 0) {
      kLog->error("start session timeout before connection ready");
      return false;
    }
    if (!EnsureConnection(connect_timeout)) {
      kLog->error("start session failed: ensure connection failed");
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(control_mu_);
      control_queue_.erase(
          std::remove_if(control_queue_.begin(), control_queue_.end(), [](const protocol::Frame& frame) {
            if (!frame.event_id.has_value()) {
              return false;
            }
            return *frame.event_id == protocol::EventId::kSessionStarted ||
                   *frame.event_id == protocol::EventId::kSessionFinished ||
                   *frame.event_id == protocol::EventId::kSessionFailed ||
                   *frame.event_id == protocol::EventId::kDialogCommonError;
          }),
          control_queue_.end());
    }

    const std::string sid = GenSessionId();
    if (!SendJsonEvent(protocol::EventId::kStartSession, payload, sid)) {
      kLog->error("start session failed: send start_session event failed");
      CloseConnection(false);
      continue;
    }

    protocol::Frame frame;
    const auto wait_timeout = remaining_timeout();
    if (wait_timeout.count() <= 0) {
      kLog->error("start session timeout while waiting session response");
      return false;
    }
    if (!WaitControlEvent({protocol::EventId::kSessionStarted, protocol::EventId::kSessionFailed,
                           protocol::EventId::kDialogCommonError},
                          sid, wait_timeout, &frame)) {
      kLog->error("start session failed: no session control event before timeout");
      CloseConnection(false);
      continue;
    }
    if (frame.event_id == protocol::EventId::kSessionFailed ||
        frame.event_id == protocol::EventId::kDialogCommonError) {
      kLog->error("start session rejected by server");
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(conn_mu_);
      session_id_ = sid;
    }
    kLog->info("session started: sid={}", sid);
    return true;
  }
  return false;
}

bool Client::FinishSession(std::chrono::milliseconds timeout) {
  std::string sid;
  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    sid = session_id_;
  }
  if (sid.empty()) {
    return true;
  }

  bool ok = SendJsonEvent(protocol::EventId::kFinishSession, nlohmann::json::object(), sid);
  bool got_session_finished = false;
  if (ok) {
    protocol::Frame frame;
    got_session_finished =
        WaitControlEvent({protocol::EventId::kSessionFinished}, sid, timeout, &frame);
  }

  // kSessionFinished is handled in HandleFrame and already triggers HandleSessionClosed.
  // Fallback to local close only when the finish event did not arrive in time.
  if (!got_session_finished) {
    HandleSessionClosed("session_finished");
  }
  return ok;
}

bool Client::EnqueueAudio(const std::vector<uint8_t>& chunk) {
  if (chunk.empty()) {
    return true;
  }
  std::lock_guard<std::mutex> lock(audio_mu_);
  if (static_cast<int>(audio_queue_.size()) >= cfg_.budget.input_queue_frames) {
    return false;
  }
  audio_queue_.emplace_back(chunk);
  audio_cv_.notify_one();
  return true;
}

bool Client::EnqueueAudio(std::vector<uint8_t>&& chunk) {
  if (chunk.empty()) {
    return true;
  }
  std::lock_guard<std::mutex> lock(audio_mu_);
  if (static_cast<int>(audio_queue_.size()) >= cfg_.budget.input_queue_frames) {
    return false;
  }
  audio_queue_.push_back(std::move(chunk));
  audio_cv_.notify_one();
  return true;
}

bool Client::SendSayHello() {
  std::string sid;
  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    sid = session_id_;
  }
  if (sid.empty()) {
    return true;
  }
  return SendJsonEvent(protocol::EventId::kSayHello,
                       nlohmann::json{{"content", cfg_.wakeup.say_hello}}, sid);
}

bool Client::EnsureConnection(std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  int attempt = 0;

  while (running_.load()) {
    {
      std::lock_guard<std::mutex> lock(conn_mu_);
      if (ws_ && ws_connected_ &&
          std::chrono::steady_clock::now() - conn_opened_at_ < kMaxConnectionAge) {
        return true;
      }
    }

    CloseConnection(false);
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return false;
    }

    if (OpenConnection(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now))) {
      return true;
    }

    ++attempt;
    const auto current = std::chrono::steady_clock::now();
    if (current >= deadline) {
      return false;
    }

    auto backoff = NextBackoff(attempt);
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - current);
    if (backoff > remaining) {
      backoff = remaining;
    }
    if (backoff.count() > 0) {
      kLog->warn("connection attempt {} failed, retrying in {}ms", attempt, backoff.count());
      std::this_thread::sleep_for(backoff);
    }
  }
  return false;
}

bool Client::OpenConnection(std::chrono::milliseconds timeout) {
  const auto& url = cfg_.realtime.preset.ws_url;

  kLog->info("connecting to {}", url);
  auto ws = std::make_unique<ix::WebSocket>();
  ws->setUrl(url);

  ws->setExtraHeaders({{"X-Api-App-ID", cfg_.realtime.app_id},
                       {"X-Api-Access-Key", cfg_.realtime.access_token},
                       {"X-Api-Resource-Id", cfg_.realtime.preset.resource_id},
                       {"X-Api-App-Key", "PlgvMymc7f3tQnJ6"}});

  ix::SocketTLSOptions tls;
  tls.caFile = "NONE";
  ws->setTLSOptions(tls);

  ws->setOnMessageCallback([this](const ix::WebSocketMessagePtr& msg) {
    if (msg->type == ix::WebSocketMessageType::Open) {
      std::lock_guard<std::mutex> lock(conn_mu_);
      ws_connected_ = true;
      conn_cv_.notify_all();
      kLog->info("connection ready");
      return;
    }

    if (msg->type == ix::WebSocketMessageType::Close ||
        msg->type == ix::WebSocketMessageType::Error) {
      if (msg->type == ix::WebSocketMessageType::Error) {
        kLog->warn("ws error: {} (retries={}, wait={}ms, http={})",
                     msg->errorInfo.reason, msg->errorInfo.retries,
                     msg->errorInfo.wait_time, msg->errorInfo.http_status);
      }
      std::string sid;
      {
        std::lock_guard<std::mutex> lock(conn_mu_);
        ws_connected_ = false;
        sid = session_id_;
        conn_cv_.notify_all();
      }
      if (!sid.empty()) {
        HandleSessionClosed("connection_lost");
      }
      control_cv_.notify_all();
      return;
    }

    if (msg->type == ix::WebSocketMessageType::Message && msg->binary) {
      try {
        std::vector<uint8_t> payload(msg->str.begin(), msg->str.end());
        auto frame = protocol::DecodeFrame(payload);
        if (frame.message_type == protocol::MessageType::kErrorInfo) {
          kLog->warn("ws server error frame");
          protocol::Frame control = frame;
          if (!control.event_id.has_value()) {
            control.event_id = protocol::EventId::kDialogCommonError;
          }
          std::lock_guard<std::mutex> lock(control_mu_);
          control_queue_.push_back(std::move(control));
          control_cv_.notify_all();
          return;
        }
        HandleFrame(frame);

        if (frame.event_id.has_value()) {
          switch (*frame.event_id) {
            case protocol::EventId::kConnectionStarted:
            case protocol::EventId::kConnectionFailed:
            case protocol::EventId::kConnectionFinished:
            case protocol::EventId::kSessionStarted:
            case protocol::EventId::kSessionFinished:
            case protocol::EventId::kSessionFailed:
            case protocol::EventId::kDialogCommonError: {
              std::lock_guard<std::mutex> lock(control_mu_);
              control_queue_.push_back(frame);
              control_cv_.notify_all();
              break;
            }
            default:
              break;
          }
        }
      } catch (const std::exception& e) {
        kLog->error("ws frame decode failed: {}", e.what());
      } catch (...) {
        kLog->error("ws frame decode failed: unknown exception");
      }
    }
  });

  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    ws_connected_ = false;
  }

  ws->start();

  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    ws_ = std::move(ws);
  }

  {
    std::lock_guard<std::mutex> lock(control_mu_);
    control_queue_.erase(
        std::remove_if(control_queue_.begin(), control_queue_.end(), [](const protocol::Frame& frame) {
          return frame.event_id.has_value() && protocol::is_connect_class_event(*frame.event_id);
        }),
        control_queue_.end());
  }

  // Wait for WebSocket to actually open before sending kStartConnection.
  {
    std::unique_lock<std::mutex> lock(conn_mu_);
    const auto ws_open_timeout = std::min(timeout, std::chrono::milliseconds(8000));
    if (!conn_cv_.wait_for(lock, ws_open_timeout, [this] { return ws_connected_; })) {
      CloseConnection(false);
      return false;
    }
  }

  if (!SendJsonEvent(protocol::EventId::kStartConnection, nlohmann::json::object(), std::nullopt)) {
    CloseConnection(false);
    return false;
  }

  protocol::Frame frame;
  const auto wait_timeout = std::min(timeout, std::chrono::milliseconds(8000));
  if (!WaitControlEvent({protocol::EventId::kConnectionStarted,
                         protocol::EventId::kConnectionFailed,
                         protocol::EventId::kDialogCommonError},
                        std::nullopt, wait_timeout, &frame)) {
    CloseConnection(false);
    return false;
  }
  if (frame.event_id == protocol::EventId::kConnectionFailed ||
      frame.event_id == protocol::EventId::kDialogCommonError) {
    CloseConnection(false);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    ws_connected_ = true;
    conn_opened_at_ = std::chrono::steady_clock::now();
  }
  return true;
}

void Client::ResetConnection() {
  // Drop the socket without a finish event; the next StartSession()
  // will run the full StartConnection handshake again.
  CloseConnection(false);
}

void Client::CloseConnection(bool send_finish_event) {
  if (send_finish_event) {
    (void)SendJsonEvent(protocol::EventId::kFinishConnection, nlohmann::json::object(), std::nullopt);
  }

  std::unique_ptr<ix::WebSocket> ws;
  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    ws_connected_ = false;
    session_id_.clear();
    ws = std::move(ws_);
  }
  if (ws) {
    try {
      ws->stop();
    } catch (const std::system_error& e) {
      kLog->error("ws stop failed with system_error: {}", e.what());
    } catch (const std::exception& e) {
      kLog->error("ws stop failed: {}", e.what());
    } catch (...) {
      kLog->error("ws stop failed: unknown exception");
    }
  }

  {
    std::lock_guard<std::mutex> lock(control_mu_);
    control_queue_.clear();
  }
}

void Client::HandleFrame(const protocol::Frame& frame) {
  if (!frame.event_id.has_value()) {
    return;
  }

  switch (*frame.event_id) {
    case protocol::EventId::kAsrResponse:
      HandleAsrResponse(frame.JsonPayload());
      break;
    case protocol::EventId::kAsrInfo:
      HandleAsrInfo();
      break;
    case protocol::EventId::kTtsResponse:
      HandleTtsResponse(frame);
      break;
    case protocol::EventId::kTtsEnded:
      HandleTtsEnded();
      break;
    case protocol::EventId::kChatResponse:
      HandleChatResponse(frame.JsonPayload());
      break;
    case protocol::EventId::kChatEnded:
      HandleChatEnded();
      break;
    case protocol::EventId::kSessionFinished:
      HandleSessionClosed("session_finished");
      break;
    case protocol::EventId::kDialogCommonError:
    case protocol::EventId::kSessionFailed:
      HandleSessionClosed("session_failed");
      break;
    default:
      break;
  }
}

void Client::HandleAsrResponse(const nlohmann::json& payload) {
  if (!payload.is_object()) {
    return;
  }
  auto it = payload.find("results");
  if (it == payload.end() || !it->is_array()) {
    return;
  }

  for (const auto& item : *it) {
    if (!item.is_object()) {
      continue;
    }
    if (AsBool(item.value("is_interim", false))) {
      continue;
    }
    if (callbacks_.on_asr_final) {
      const auto text = AsString(item.value("text", ""));
      kLog->info("asr final: '{}'", text);
      callbacks_.on_asr_final(text);
    }
  }
}

void Client::HandleAsrInfo() {
  // Use ASRInfo (event 450) as the single barge-in signal.
  if (callbacks_.on_user_activity) {
    callbacks_.on_user_activity();
  }
}

void Client::HandleTtsResponse(const protocol::Frame& frame) {
  bool set_speaking = false;
  {
    std::lock_guard<std::mutex> lock(event_mu_);
    if (!is_ai_speaking_) {
      is_ai_speaking_ = true;
      set_speaking = true;
    }
  }

  if (set_speaking && callbacks_.on_set_ai_speaking) {
    kLog->info("tts started");
    callbacks_.on_set_ai_speaking(true);
  }
  if (callbacks_.on_audio && !frame.payload.empty()) {
    callbacks_.on_audio(frame.payload);
  }
}

void Client::HandleTtsEnded() {
  bool was_speaking = false;
  {
    std::lock_guard<std::mutex> lock(event_mu_);
    if (is_ai_speaking_) {
      is_ai_speaking_ = false;
      was_speaking = true;
    }
  }
  if (was_speaking && callbacks_.on_set_ai_speaking) {
    kLog->info("tts ended");
    callbacks_.on_set_ai_speaking(false);
  }
}

void Client::HandleChatResponse(const nlohmann::json& payload) {
  if (!payload.is_object()) {
    return;
  }
}

void Client::HandleChatEnded() {
  if (callbacks_.on_chat_ended) {
    callbacks_.on_chat_ended();
  }
}

void Client::HandleSessionClosed(const std::string& reason) {
  {
    std::lock_guard<std::mutex> lock(conn_mu_);
    session_id_.clear();
  }
  {
    std::lock_guard<std::mutex> lock(event_mu_);
    is_ai_speaking_ = false;
  }
  if (callbacks_.on_set_ai_speaking) {
    callbacks_.on_set_ai_speaking(false);
  }
  if (callbacks_.on_session_closed) {
    callbacks_.on_session_closed(reason);
  }
}

bool Client::SendJsonEvent(protocol::EventId event_id, const nlohmann::json& payload,
                           const std::optional<std::string>& session_id) {
  std::vector<uint8_t> frame;
  try {
    frame = protocol::BuildJsonFrame(protocol::MessageType::kFullClientRequest, event_id, payload,
                                     session_id);
  } catch (...) {
    kLog->error("json frame build failed");
    return false;
  }
  return WriteBinary(frame);
}

bool Client::WriteBinary(const std::vector<uint8_t>& payload) {
  std::lock_guard<std::mutex> lock(write_mu_);
  std::lock_guard<std::mutex> conn_lock(conn_mu_);
  if (!ws_) {
    return false;
  }
  auto res = ws_->sendBinary(std::string(reinterpret_cast<const char*>(payload.data()), payload.size()));
  return res.success;
}

bool Client::WaitControlEvent(const std::vector<protocol::EventId>& targets,
                              const std::optional<std::string>& session_id,
                              std::chrono::milliseconds timeout, protocol::Frame* out_frame) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  auto is_target = [&targets](protocol::EventId event_id) {
    return std::find(targets.begin(), targets.end(), event_id) != targets.end();
  };

  std::unique_lock<std::mutex> lock(control_mu_);
  while (running_.load()) {
    for (auto it = control_queue_.begin(); it != control_queue_.end(); ++it) {
      if (!it->event_id.has_value()) {
        continue;
      }
      if (!is_target(*it->event_id)) {
        continue;
      }
      if (session_id.has_value()) {
        if (it->session_id.has_value()) {
          if (*it->session_id != *session_id) {
            continue;
          }
        } else {
          // Some server-side frames may omit session_id.
          if (*it->event_id != protocol::EventId::kSessionStarted &&
              *it->event_id != protocol::EventId::kSessionFinished &&
              *it->event_id != protocol::EventId::kSessionFailed &&
              *it->event_id != protocol::EventId::kDialogCommonError) {
            continue;
          }
        }
      }
      if (out_frame) {
        *out_frame = *it;
      }
      control_queue_.erase(it);
      return true;
    }

    if (control_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return false;
    }
  }
  return false;
}

std::chrono::milliseconds Client::NextBackoff(int attempt) const {
  int min_ms = cfg_.budget.reconnect_backoff_min_ms;
  int max_ms = cfg_.budget.reconnect_backoff_max_ms;
  if (min_ms <= 0) {
    min_ms = 300;
  }
  if (max_ms < min_ms) {
    max_ms = min_ms * 4;
  }

  int upper = min_ms << std::max(0, attempt - 1);
  upper = std::min(upper, max_ms);
  if (upper <= min_ms) {
    return std::chrono::milliseconds(min_ms);
  }

  std::lock_guard<std::mutex> lock(rng_mu_);
  std::uniform_int_distribution<int> dist(min_ms, upper);
  return std::chrono::milliseconds(dist(rng_));
}

}  // namespace xiaoai_plus::realtime
