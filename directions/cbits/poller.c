/*
 * A poller for one scheduler thread: descriptors registered once, for edges
 * of readability and writability both, and one call that waits for any of
 * them. kqueue on macOS and the BSDs, epoll on Linux.
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

#define DIRECTIONS_READABLE 1
#define DIRECTIONS_WRITABLE 2
#define DIRECTIONS_EVENTS 64

typedef struct directions_poller {
  int fd;
  int wake; /* the eventfd on Linux, unused on kqueue */
} directions_poller;

/* A monotonic clock, in nanoseconds. macOS's CLOCK_MONOTONIC ticks in whole
 * microseconds, so there it is the uptime clock, which doesn't. */
uint64_t directions_clock_ns(void) {
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

#define DIRECTIONS_WAKE_IDENT 1

directions_poller *directions_poller_new(void) {
  directions_poller *poller = malloc(sizeof(directions_poller));
  if (poller == NULL) return NULL;
  poller->wake = -1;
  poller->fd = kqueue();
  if (poller->fd < 0) {
    free(poller);
    return NULL;
  }
  struct kevent change;
  EV_SET(&change, DIRECTIONS_WAKE_IDENT, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, 0);
  if (kevent(poller->fd, &change, 1, NULL, 0, NULL) < 0) {
    int saved = errno;
    close(poller->fd);
    free(poller);
    errno = saved;
    return NULL;
  }
  return poller;
}

void directions_poller_free(directions_poller *poller) {
  close(poller->fd);
  free(poller);
}

int directions_poller_add(directions_poller *poller, int fd) {
  struct kevent changes[2];
  EV_SET(&changes[0], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, 0);
  EV_SET(&changes[1], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, 0);
  return kevent(poller->fd, changes, 2, NULL, 0, NULL);
}

/* From any thread: ends the wait of the thread that owns the poller, or the
 * next one if it isn't waiting. */
int directions_poller_wake(directions_poller *poller) {
  struct kevent change;
  EV_SET(&change, DIRECTIONS_WAKE_IDENT, EVFILT_USER, 0, NOTE_TRIGGER, 0, 0);
  return kevent(poller->fd, &change, 1, NULL, 0, NULL);
}

int directions_poller_wait(directions_poller *poller, int *fds, int *kinds, int max, int timeout_ms) {
  struct kevent events[DIRECTIONS_EVENTS];
  struct timespec timeout, *until = NULL;
  if (max > DIRECTIONS_EVENTS) max = DIRECTIONS_EVENTS;
  if (timeout_ms >= 0) {
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
    until = &timeout;
  }
  int count = kevent(poller->fd, NULL, 0, events, max, until);
  int reported = 0;
  for (int i = 0; i < count; i++) {
    if (events[i].filter == EVFILT_USER) continue;
    fds[reported] = (int)events[i].ident;
    int kind = events[i].filter == EVFILT_READ ? DIRECTIONS_READABLE : DIRECTIONS_WRITABLE;
    if (events[i].flags & (EV_EOF | EV_ERROR)) kind = DIRECTIONS_READABLE | DIRECTIONS_WRITABLE;
    kinds[reported] = kind;
    reported++;
  }
  return count < 0 ? count : reported;
}

#elif defined(__linux__)
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

directions_poller *directions_poller_new(void) {
  directions_poller *poller = malloc(sizeof(directions_poller));
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

void directions_poller_free(directions_poller *poller) {
  close(poller->wake);
  close(poller->fd);
  free(poller);
}

int directions_poller_add(directions_poller *poller, int fd) {
  struct epoll_event event;
  event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
  event.data.fd = fd;
  return epoll_ctl(poller->fd, EPOLL_CTL_ADD, fd, &event);
}

/* From any thread: ends the wait of the thread that owns the poller, or the
 * next one if it isn't waiting. */
int directions_poller_wake(directions_poller *poller) {
  uint64_t one = 1;
  ssize_t written = write(poller->wake, &one, sizeof(one));
  /* A counter at its limit has a wake pending already. */
  return written == (ssize_t)sizeof(one) || errno == EAGAIN ? 0 : -1;
}

int directions_poller_wait(directions_poller *poller, int *fds, int *kinds, int max, int timeout_ms) {
  struct epoll_event events[DIRECTIONS_EVENTS];
  if (max > DIRECTIONS_EVENTS) max = DIRECTIONS_EVENTS;
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
    if (happened & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) kind |= DIRECTIONS_READABLE;
    if (happened & (EPOLLOUT | EPOLLHUP | EPOLLERR)) kind |= DIRECTIONS_WRITABLE;
    fds[reported] = events[i].data.fd;
    kinds[reported] = kind;
    reported++;
  }
  return count < 0 ? count : reported;
}

#else
#error "directions: no poller for this platform"
#endif
