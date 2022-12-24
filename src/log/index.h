#pragma once

#include "absl/container/flat_hash_map.h"
#include "log/log_space_base.h"
#include <cstdint>
#include <vector>

namespace faas { namespace log {

struct IndexFoundResult {
    uint16_t view_id;
    uint16_t engine_id;
    uint64_t seqnum;
};

struct IndexQuery {
    enum ReadDirection { kReadNext, kReadPrev, kReadNextB };
    ReadDirection direction;
    uint16_t origin_node_id;
    uint16_t hop_times;
    bool initial;
    uint64_t client_data;

    uint32_t user_logspace;
    uint64_t user_tag;
    uint64_t query_seqnum;
    uint64_t metalog_progress;

    IndexFoundResult prev_found_result;

    static ReadDirection DirectionFromOpType(protocol::SharedLogOpType op_type);
    protocol::SharedLogOpType DirectionToOpType() const;
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kContinue };
    State state;
    uint64_t metalog_progress;
    uint16_t next_view_id;

    IndexQuery original_query;
    IndexFoundResult found_result;
};

class CCIndex final: public LogSpaceBase {
public:
    CCIndex(const View* view, uint16_t sequencer_id);
    ~CCIndex();

    uint64_t finished_snapshot_ = 0;
    uint64_t current_seqnum_ = 1;
    void ApplyCCGlobalBatch(const CCGlobalBatchProto& batch_proto);
    void ProvideCCGlobalBatch(const CCGlobalBatchProto& batch_proto,
                              uint64_t finished_snapshot);

    struct IndexEntry {
        bool has_aux = false;
        // uint32_t global_batch_position;
        uint64_t seqnum_;
        // for now, use flat index, so we don't need seqnum
        // TODO: add seqnum in global batch ? or perhaps return number of non-flat
        // index in this batch
        uint64_t txn_id;
        IndexEntry() {}
        IndexEntry(uint64_t seqnum, uint64_t txn_id)
            : seqnum_(seqnum),
              txn_id(txn_id)
        {}
    };
    absl::flat_hash_map<uint64_t, std::vector<IndexEntry>> index_;
    // readerlock is suffcient for now
    IndexEntry Lookup(uint64_t key, uint64_t global_batch_id) const;
    // void pollLookupResult
    // absl::flat_hash_map<uint64_t, std::vector<uint64_t>> snapshots_;
    // void MarkSnapshot(uint64_t key, uint64_t global_batch_id);
};

class Index final: public LogSpaceBase {
public:
    static constexpr absl::Duration kBlockingQueryTimeout = absl::Seconds(1);

    Index(const View* view, uint16_t sequencer_id);
    ~Index();

    void ProvideIndexData(const IndexDataProto& index_data);

    void MakeQuery(const IndexQuery& query);

    using QueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;
    void PollQueryResults(QueryResultVec* results);

private:
    class PerSpaceIndex;
    absl::flat_hash_map</* user_logspace */ uint32_t, std::unique_ptr<PerSpaceIndex>>
        index_;

    static constexpr uint32_t kMaxMetalogPosition =
        std::numeric_limits<uint32_t>::max();

    std::multimap</* metalog_position */ uint32_t, IndexQuery> pending_queries_;
    std::vector<std::pair</* start_timestamp */ int64_t, IndexQuery>>
        blocking_reads_;
    QueryResultVec pending_query_results_;

    std::deque<std::pair</* metalog_seqnum */ uint32_t,
                         /* end_seqnum */ uint32_t>>
        cuts_;
    uint32_t indexed_metalog_position_;

    struct IndexData {
        uint16_t engine_id;
        uint32_t user_logspace;
        UserTagVec user_tags;
    };
    std::map</* seqnum */ uint32_t, IndexData> received_data_;
    uint32_t data_received_seqnum_position_;
    uint32_t indexed_seqnum_position_;

    uint64_t index_metalog_progress() const
    {
        return bits::JoinTwo32(identifier(), indexed_metalog_position_);
    }

    void OnMetaLogApplied(const MetaLogProto& meta_log_proto) override;
    void OnFinalized(uint32_t metalog_position) override;
    void AdvanceIndexProgress();
    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);

    void ProcessQuery(const IndexQuery& query);
    void ProcessReadNext(const IndexQuery& query);
    void ProcessReadPrev(const IndexQuery& query);
    bool ProcessBlockingQuery(const IndexQuery& query);

    bool IndexFindNext(const IndexQuery& query,
                       uint64_t* seqnum,
                       uint16_t* engine_id);
    bool IndexFindPrev(const IndexQuery& query,
                       uint64_t* seqnum,
                       uint16_t* engine_id);

    IndexQueryResult BuildFoundResult(const IndexQuery& query,
                                      uint16_t view_id,
                                      uint64_t seqnum,
                                      uint16_t engine_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);
    IndexQueryResult BuildContinueResult(const IndexQuery& query,
                                         bool found,
                                         uint64_t seqnum,
                                         uint16_t engine_id);

    DISALLOW_COPY_AND_ASSIGN(Index);
};

}} // namespace faas::log
