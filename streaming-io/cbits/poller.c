/*
 * A poller for one scheduler thread: descriptors that are armed when a part
 * waits on them, for one report each, and one call that waits for any of them.
 * A descriptor is armed for the directions, readable and writable, that parts
 * wait for, and is reported once: arming it again is what a part's next wait
 * does. Arming reports the readiness that is already there, as a new
 * descriptor's is, so a part that waits for input that arrived before it did
 * is woken; and a descriptor number that a new socket got after a close needs
 * nothing but the wait. kqueue on macOS and the BSDs, epoll on Linux.
 *
 * The poller also has a wake handle, registered with it once when it is made,
 * which any other thread can trigger to end the scheduler's wait: an
 * EVFILT_USER event on kqueue, an eventfd on epoll. A wake is not a
 * descriptor's readiness: the wait takes it off and reports nothing for it.
 *
 * It also has the clock that every instant and every turn is read from.
 */
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define STREAMING_IO_READABLE 1
#define STREAMING_IO_WRITABLE 2
#define STREAMING_IO_EVENTS 64

/* A monotonic clock, in nanoseconds. macOS's CLOCK_MONOTONIC ticks in whole
 * microseconds, so there it is the uptime clock, which doesn't. */
uint64_t streaming_io_clock_ns(void) {
#ifdef __APPLE__
  return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
#else
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
#endif
}

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include <sys/event.h>
#include <sys/types.h>

#define STREAMING_IO_WAKE_IDENT 1
#define STREAMING_IO_FIRST_CHANGES 64

typedef struct streaming_io_poller {
  int fd;
  /* The armings that the next wait submits with its own call: a part's wait
   * costs no system call of its own. */
  struct kevent *changes;
  int changes_count;
  int changes_capacity;
} streaming_io_poller;

streaming_io_poller *streaming_io_poller_new(void) {
  streaming_io_poller *poller = malloc(sizeof(streaming_io_poller));
  if (poller == NULL) return NULL;
  poller->changes_count = 0;
  poller->changes_capacity = STREAMING_IO_FIRST_CHANGES;
  poller->changes = malloc(sizeof(struct kevent) * poller->changes_capacity);
  if (poller->changes == NULL) {
    free(poller);
    return NULL;
  }
  poller->fd = kqueue();
  if (poller->fd < 0) {
    free(poller->changes);
    free(poller);
    return NULL;
  }
  struct kevent change;
  EV_SET(&change, STREAMING_IO_WAKE_IDENT, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, 0);
  if (kevent(poller->fd, &change, 1, NULL, 0, NULL) < 0) {
    int saved = errno;
    close(poller->fd);
    free(poller->changes);
    free(poller);
    errno = saved;
    return NULL;
  }
  return poller;
}

void streaming_io_poller_free(streaming_io_poller *poller) {
  close(poller->fd);
  free(poller->changes);
  free(poller);
}

/* Arms the descriptor for the kinds in added, those that parts began to wait
 * for. Each filter has a registration of its own that reports once, so it goes
 * in the list of changes that the next wait submits with its own call. A filter
 * that is armed already is armed again, which evaluates it again. What parts
 * still wait for after a report needs nothing, since the filters that did not
 * report are armed still: waiting is not used here. A change that fails, for a
 * descriptor that was closed meanwhile, comes back from the wait as an event
 * for the descriptor. */
int streaming_io_poller_arm(streaming_io_poller *poller, int fd, int added, int waiting) {
  (void)waiting;
  int needed = ((added & STREAMING_IO_READABLE) != 0) + ((added & STREAMING_IO_WRITABLE) != 0);
  if (needed == 0) return 0;
  if (poller->changes_count + needed > poller->changes_capacity) {
    int capacity = poller->changes_capacity * 2;
    struct kevent *grown = realloc(poller->changes, sizeof(struct kevent) * capacity);
    if (grown == NULL) {
      errno = ENOMEM;
      return -1;
    }
    poller->changes = grown;
    poller->changes_capacity = capacity;
  }
  if (added & STREAMING_IO_READABLE)
    EV_SET(&poller->changes[poller->changes_count++], fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, 0);
  if (added & STREAMING_IO_WRITABLE)
    EV_SET(&poller->changes[poller->changes_count++], fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, 0);
  return 0;
}

/* From any thread: ends the wait of the thread that owns the poller, or the
 * next one if it isn't waiting. */
int streaming_io_poller_wake(streaming_io_poller *poller) {
  struct kevent change;
  EV_SET(&change, STREAMING_IO_WAKE_IDENT, EVFILT_USER, 0, NOTE_TRIGGER, 0, 0);
  return kevent(poller->fd, &change, 1, NULL, 0, NULL);
}

int streaming_io_poller_wait(streaming_io_poller *poller, int *fds, int *kinds, int max, int timeout_ms) {
  struct kevent events[STREAMING_IO_EVENTS];
  struct timespec timeout, *until = NULL;
  if (max > STREAMING_IO_EVENTS) max = STREAMING_IO_EVENTS;
  if (timeout_ms >= 0) {
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
    until = &timeout;
  }
  int count = kevent(poller->fd, poller->changes, poller->changes_count, events, max, until);
  /* A wait that a signal ended may not have taken the changes: they go again. */
  if (count >= 0 || errno != EINTR) poller->changes_count = 0;
  int reported = 0;
  for (int i = 0; i < count; i++) {
    if (events[i].filter == EVFILT_USER) continue;
    fds[reported] = (int)events[i].ident;
    int kind = events[i].filter == EVFILT_READ ? STREAMING_IO_READABLE : STREAMING_IO_WRITABLE;
    if (events[i].flags & (EV_EOF | EV_ERROR)) kind = STREAMING_IO_READABLE | STREAMING_IO_WRITABLE;
    kinds[reported] = kind;
    reported++;
  }
  return count < 0 ? count : reported;
}

#elif defined(__linux__)
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

typedef struct streaming_io_poller {
  int fd;
  int wake; /* the eventfd */
} streaming_io_poller;

streaming_io_poller *streaming_io_poller_new(void) {
  streaming_io_poller *poller = malloc(sizeof(streaming_io_poller));
  if (poller == NULL) return NULL;
  poller->fd = epoll_create1(EPOLL_CLOEXEC);
  if (poller->fd < 0) {
    free(poller);
    return NULL;
  }
  poller->wake = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  struct epoll_event event;
  event.events = EPOLLIN;
  event.data.fd = poller->wake;
  if (poller->wake < 0 || epoll_ctl(poller->fd, EPOLL_CTL_ADD, poller->wake, &event) < 0) {
    int saved = errno;
    if (poller->wake >= 0) close(poller->wake);
    close(poller->fd);
    free(poller);
    errno = saved;
    return NULL;
  }
  return poller;
}

void streaming_io_poller_free(streaming_io_poller *poller) {
  close(poller->wake);
  close(poller->fd);
  free(poller);
}

/* Arms the descriptor for the kinds in waiting, all that parts wait for, once.
 * A descriptor has one registration, for both kinds, which a report disables,
 * so it is armed again for what parts still wait for after one, and then added
 * is not needed. Modifying a registration that is there arms it and evaluates
 * it again, so that readiness already there is reported; one that isn't there,
 * for a descriptor that was not armed before, or that was closed and its number
 * reused, is added. A descriptor that waits again is far more often one that is
 * registered, so the modification is tried first, which costs one system call. */
int streaming_io_poller_arm(streaming_io_poller *poller, int fd, int added, int waiting) {
  (void)added;
  if (waiting == 0) return 0;
  struct epoll_event event;
  event.events = EPOLLONESHOT;
  if (waiting & STREAMING_IO_READABLE) event.events |= EPOLLIN | EPOLLRDHUP;
  if (waiting & STREAMING_IO_WRITABLE) event.events |= EPOLLOUT;
  event.data.fd = fd;
  if (epoll_ctl(poller->fd, EPOLL_CTL_MOD, fd, &event) == 0) return 0;
  if (errno != ENOENT) return -1;
  return epoll_ctl(poller->fd, EPOLL_CTL_ADD, fd, &event);
}

/* From any thread: ends the wait of the thread that owns the poller, or the
 * next one if it isn't waiting. */
int streaming_io_poller_wake(streaming_io_poller *poller) {
  uint64_t one = 1;
  ssize_t written = write(poller->wake, &one, sizeof(one));
  /* A counter at its limit has a wake pending already. */
  return written == (ssize_t)sizeof(one) || errno == EAGAIN ? 0 : -1;
}

int streaming_io_poller_wait(streaming_io_poller *poller, int *fds, int *kinds, int max, int timeout_ms) {
  struct epoll_event events[STREAMING_IO_EVENTS];
  if (max > STREAMING_IO_EVENTS) max = STREAMING_IO_EVENTS;
  int count = epoll_wait(poller->fd, events, max, timeout_ms);
  int reported = 0;
  for (int i = 0; i < count; i++) {
    if (events[i].data.fd == poller->wake) {
      uint64_t taken;
      ssize_t ignored = read(poller->wake, &taken, sizeof(taken));
      (void)ignored;
      continue;
    }
    unsigned int happened = events[i].events;
    int kind = 0;
    if (happened & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) kind |= STREAMING_IO_READABLE;
    if (happened & (EPOLLOUT | EPOLLHUP | EPOLLERR)) kind |= STREAMING_IO_WRITABLE;
    fds[reported] = events[i].data.fd;
    kinds[reported] = kind;
    reported++;
  }
  return count < 0 ? count : reported;
}

#else
#error "streaming-io: no poller for this platform"
#endif
