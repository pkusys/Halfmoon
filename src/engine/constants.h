#pragma once

namespace faas {
namespace engine {

constexpr int kGatewayConnectionTypeId      = 0 << 16;
constexpr int kMessageConnectionTypeId      = 1 << 16;
constexpr int kSequencerConnectionTypeId    = 2 << 16;
constexpr int kIncomingSLogConnectionTypeId = 3 << 16;
constexpr int kSLogMessageHubTypeId         = 4 << 16;
constexpr int kSLogEngineTimerTypeId        = 5 << 16;

constexpr uint16_t kGatewayConnectionBufGroup      = 1;
constexpr uint16_t kMessageConnectionBufGroup      = 2;
constexpr uint16_t kSequencerConnectionBufGroup    = 3;
constexpr uint16_t kIncomingSLogConnectionBufGroup = 4;

}  // namespace engine
}  // namespace faas