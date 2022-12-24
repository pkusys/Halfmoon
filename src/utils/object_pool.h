#pragma once

#include "base/common.h"

#ifdef __FAAS_HAVE_ABSL
__BEGIN_THIRD_PARTY_HEADERS
#include <absl/container/inlined_vector.h>
#include <absl/synchronization/mutex.h>
__END_THIRD_PARTY_HEADERS
#endif

#ifdef __FAAS_SRC
__BEGIN_THIRD_PARTY_HEADERS
#include <google/protobuf/arena.h>
__END_THIRD_PARTY_HEADERS
#endif

namespace faas { namespace utils {

using Arena = google::protobuf::Arena;

template <class T>
T*
DefaultObjectConstructor()
{
    return new T();
}

// SimpleObjectPool is NOT thread-safe
template <class T>
class SimpleObjectPool {
public:
    explicit SimpleObjectPool(
        std::function<T*()> object_constructor = DefaultObjectConstructor<T>)
        : object_constructor_(object_constructor)
    {}

    ~SimpleObjectPool() {}

    T* Get()
    {
        if (free_objs_.empty()) {
            T* new_obj = object_constructor_();
            free_objs_.push_back(new_obj);
            objs_.emplace_back(new_obj);
        }
        DCHECK(!free_objs_.empty());
        T* obj = free_objs_.back();
        free_objs_.pop_back();
        return obj;
    }

    void Return(T* obj) { free_objs_.push_back(obj); }

private:
    std::function<T*()> object_constructor_;
#ifdef __FAAS_HAVE_ABSL
    absl::InlinedVector<std::unique_ptr<T>, 16> objs_;
    absl::InlinedVector<T*, 16> free_objs_;
#else
    std::vector<std::unique_ptr<T>> objs_;
    std::vector<T*> free_objs_;
#endif

    DISALLOW_COPY_AND_ASSIGN(SimpleObjectPool);
};

#ifdef __FAAS_SRC

template <class T>
class ProtobufMessagePool {
public:
    ProtobufMessagePool() {}
    ~ProtobufMessagePool() {}

    T* Get()
    {
        if (free_objs_.empty()) {
            free_objs_.push_back(google::protobuf::Arena::CreateMessage<T>(&arena_));
        }
        T* obj = free_objs_.back();
        free_objs_.pop_back();
        return obj;
    }

    void Return(T* obj) { free_objs_.push_back(obj); }

private:
    google::protobuf::Arena arena_;
    absl::InlinedVector<T*, 16> free_objs_;

    DISALLOW_COPY_AND_ASSIGN(ProtobufMessagePool);
};

template <class T>
class ProtobufMessagePoolWithArena {
public:
    ProtobufMessagePoolWithArena() {}

    void SetArena(Arena* arena) { arena_ = arena; }

    ~ProtobufMessagePoolWithArena() {}

    T* Get()
    {
        if (free_objs_.empty()) {
            free_objs_.push_back(Arena::CreateMessage<T>(arena_));
        }
        T* obj = free_objs_.back();
        free_objs_.pop_back();
        return obj;
    }

    void Return(T* obj) { free_objs_.push_back(obj); }

private:
    Arena* arena_;
    absl::InlinedVector<T*, 16> free_objs_;

    DISALLOW_COPY_AND_ASSIGN(ProtobufMessagePoolWithArena);
};

template <class T>
class ThreadSafeObjectPool {
public:
    explicit ThreadSafeObjectPool(
        std::function<T*()> object_constructor = DefaultObjectConstructor<T>)
        : object_constructor_(object_constructor)
    {}

    ~ThreadSafeObjectPool() {}

    T* Get()
    {
        absl::MutexLock lk(&mu_);
        if (free_objs_.empty()) {
            T* new_obj = object_constructor_();
            free_objs_.push_back(new_obj);
            objs_.emplace_back(new_obj);
        }
        DCHECK(!free_objs_.empty());
        T* obj = free_objs_.back();
        free_objs_.pop_back();
        return obj;
    }

    void Return(T* obj)
    {
        absl::MutexLock lk(&mu_);
        free_objs_.push_back(obj);
    }

private:
    std::function<T*()> object_constructor_;

    absl::Mutex mu_;
    absl::InlinedVector<std::unique_ptr<T>, 16> objs_ ABSL_GUARDED_BY(mu_);
    absl::InlinedVector<T*, 16> free_objs_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadSafeObjectPool);
};

#endif // __FAAS_SRC

}} // namespace faas::utils
