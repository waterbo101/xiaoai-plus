#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <ixwebsocket/IXWebSocket.h>

#include "config/config.hpp"
#include "realtime/frame_codec.hpp"

namespace xiaoai_plus::realtime {

class Client {
 public:
  struct Callbacks {
    std::function<void(const std::vector<uint8_t>&)> on_audio;
    std::function<void(bool)> on_set_ai_speaking;
    std::function<void(const std::string&)> on_asr_final;
    std::function<void()> on_user_activity;
    std::function<void(const std::string&)> on_session_closed;
    std::function<void()> on_chat_ended;
  };

  explicit Client(config::Config cfg, Callbacks callbacks);
  ~Client();

  bool Start();
  void Stop();

  bool StartSession(std::chrono::milliseconds timeout = std::chrono::seconds(10));
  bool FinishSession(std::chrono::milliseconds timeout = std::chrono::seconds(2));
  bool EnqueueAudio(const std::vector<uint8_t>& chunk);
  bool EnqueueAudio(std::vector<uint8_t>&& chunk);
  bool SendSayHello();
  void ResetConnection();

 private:
  bool EnsureConnection(std::chrono::milliseconds timeout);
  bool OpenConnection(std::chrono::milliseconds timeout);
  void CloseConnection(bool send_finish_event);
  void HandleFrame(const protocol::Frame& frame);
  void HandleAsrResponse(const nlohmann::json& payload);
  void HandleAsrInfo();
  void HandleTtsResponse(const protocol::Frame& frame);
  void HandleTtsEnded();
  void HandleChatResponse(const nlohmann::json& payload);
  void HandleChatEnded();
  void HandleSessionClosed(const std::string& reason);

  bool SendJsonEvent(protocol::EventId event_id, const nlohmann::json& payload,
                     const std::optional<std::string>& session_id);
  bool WriteBinary(const std::vector<uint8_t>& payload);
  bool WaitControlEvent(const std::vector<protocol::EventId>& targets,
                        const std::optional<std::string>& session_id,
                        std::chrono::milliseconds timeout, protocol::Frame* out_frame);
  std::chrono::milliseconds NextBackoff(int attempt) const;

  config::Config cfg_;
  Callbacks callbacks_;

  mutable std::mutex conn_mu_;
  std::condition_variable conn_cv_;
  std::unique_ptr<ix::WebSocket> ws_;
  bool ws_connected_{false};
  std::chrono::steady_clock::time_point conn_opened_at_{};
  std::string session_id_;

  mutable std::mutex write_mu_;

  mutable std::mutex audio_mu_;
  std::condition_variable audio_cv_;
  std::deque<std::vector<uint8_t>> audio_queue_;

  mutable std::mutex control_mu_;
  std::condition_variable control_cv_;
  std::deque<protocol::Frame> control_queue_;

  mutable std::mutex event_mu_;
  bool is_ai_speaking_{false};

  mutable std::mutex rng_mu_;
  mutable std::mt19937 rng_;

  std::atomic<bool> running_{false};
  std::thread sender_thread_;
};

}  // namespace xiaoai_plus::realtime
