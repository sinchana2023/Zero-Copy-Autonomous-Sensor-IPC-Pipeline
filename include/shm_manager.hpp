#pragma once

// RAII owner for one POSIX shared-memory segment (shm_open, ftruncate, mmap).
// The creating process unlinks the name on destruction and from SIGINT/SIGTERM
// so a killed benchmark does not leave an object in /dev/shm.

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace zcipc {

class ShmManager {
public:
    static volatile std::sig_atomic_t stop_requested;

    ShmManager() = default;
    ~ShmManager() { close(); }

    ShmManager(const ShmManager&) = delete;
    ShmManager& operator=(const ShmManager&) = delete;

    // `name` must be a POSIX shm name ("/example"). On failure errno is preserved
    // and the object stays closed. Creating unlinks a stale name first.
    bool open(const char* name, std::size_t length, bool create) noexcept {
        close();
        if (name == nullptr || name[0] != '/' || std::strchr(name + 1, '/') != nullptr) {
            errno = EINVAL;
            return false;
        }
        if (std::strlen(name) >= sizeof(name_)) {
            errno = ENAMETOOLONG;
            return false;
        }
        std::memcpy(name_, name, std::strlen(name) + 1);
        length_ = length;
        owner_ = create;

        int flags = O_RDWR | O_CLOEXEC;
        if (create) {
            shm_unlink(name_);
            flags |= O_CREAT | O_EXCL;
        }
        fd_ = ::shm_open(name_, flags, 0600);
        if (fd_ < 0) {
            owner_ = false;
            name_[0] = '\0';
            return false;
        }
        if (create) {
            if (::ftruncate(fd_, static_cast<off_t>(length_)) != 0) {
                fail_open();
                return false;
            }
        } else {
            struct stat st {};
            if (::fstat(fd_, &st) != 0 || static_cast<std::size_t>(st.st_size) < length_) {
                if (errno == 0) {
                    errno = EINVAL;
                }
                fail_open();
                return false;
            }
        }

        void* mapped = ::mmap(nullptr, length_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mapped == MAP_FAILED) {
            fail_open();
            return false;
        }
        map_ = mapped;
        // The mapping holds the last reference. Close the fd so valgrind
        // does not report a leaked descriptor.
        ::close(fd_);
        fd_ = -1;

        if (owner_) {
            publish_unlink_name();
        }
        return true;
    }

    void close() noexcept {
        if (map_ != nullptr) {
            ::munmap(map_, length_);
            map_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (owner_ && name_[0] != '\0') {
            ::shm_unlink(name_);
            if (std::strcmp(unlink_name_, name_) == 0) {
                unlink_ready_ = 0;
            }
            owner_ = false;
        }
        name_[0] = '\0';
        length_ = 0;
    }

    bool try_mlock() noexcept {
        if (map_ == nullptr) {
            errno = EINVAL;
            return false;
        }
        return ::mlock(map_, length_) == 0;
    }

    void* data() const noexcept { return map_; }
    std::size_t size() const noexcept { return length_; }
    const char* name() const noexcept { return name_; }
    bool owner() const noexcept { return owner_; }
    explicit operator bool() const noexcept { return map_ != nullptr; }

    static void install_signal_handlers() noexcept {
        struct sigaction sa {};
        sa.sa_handler = &ShmManager::handle_signal;
        sigemptyset(&sa.sa_mask);
        // Do not restart so clock_nanosleep and friends return EINTR.
        sa.sa_flags = 0;
        sigaction(SIGINT, &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
    }

private:
    static void handle_signal(int) noexcept {
        stop_requested = 1;
        if (unlink_ready_ != 0 && unlink_name_[0] != '\0') {
            ::shm_unlink(unlink_name_);
        }
    }

    void publish_unlink_name() noexcept {
        std::memcpy(unlink_name_, name_, sizeof(unlink_name_));
        std::atomic_signal_fence(std::memory_order_release);
        unlink_ready_ = 1;
    }

    void fail_open() noexcept {
        const int saved = errno;
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (owner_ && name_[0] != '\0') {
            ::shm_unlink(name_);
        }
        owner_ = false;
        name_[0] = '\0';
        errno = saved;
    }

    char name_[128]{};
    int fd_ = -1;
    void* map_ = nullptr;
    std::size_t length_ = 0;
    bool owner_ = false;

    static char unlink_name_[128];
    static volatile std::sig_atomic_t unlink_ready_;
};

inline volatile std::sig_atomic_t ShmManager::stop_requested = 0;
inline char ShmManager::unlink_name_[128] = {};
inline volatile std::sig_atomic_t ShmManager::unlink_ready_ = 0;

}  // namespace zcipc
