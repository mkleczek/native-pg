/*
 * A poller for one scheduler thread: descriptors registered once, for edges
 * of readability and writability both, and one call that waits for any of
 * them. kqueue on macOS and the BSDs, epoll on Linux.
 */
#include <errno.h>
#include <time.h>
#include <unistd.h>

#define DIRECTIONS_READABLE 1
#define DIRECTIONS_WRITABLE 2
#define DIRECTIONS_EVENTS 64

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include <sys/event.h>
#include <sys/types.h>

int directions_poller_new(void) { return kqueue(); }

int directions_poller_add(int poller, int fd) {
  struct kevent changes[2];
  EV_SET(&changes[0], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, 0);
  EV_SET(&changes[1], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, 0);
  return kevent(poller, changes, 2, NULL, 0, NULL);
}

int directions_poller_wait(int poller, int *fds, int *kinds, int max, int timeout_ms) {
  struct kevent events[DIRECTIONS_EVENTS];
  struct timespec timeout, *until = NULL;
  if (max > DIRECTIONS_EVENTS) max = DIRECTIONS_EVENTS;
  if (timeout_ms >= 0) {
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
    until = &timeout;
  }
  int count = kevent(poller, NULL, 0, events, max, until);
  for (int i = 0; i < count; i++) {
    fds[i] = (int)events[i].ident;
    int kind = events[i].filter == EVFILT_READ ? DIRECTIONS_READABLE : DIRECTIONS_WRITABLE;
    if (events[i].flags & (EV_EOF | EV_ERROR)) kind = DIRECTIONS_READABLE | DIRECTIONS_WRITABLE;
    kinds[i] = kind;
  }
  return count;
}

#elif defined(__linux__)
#include <sys/epoll.h>

int directions_poller_new(void) { return epoll_create1(EPOLL_CLOEXEC); }

int directions_poller_add(int poller, int fd) {
  struct epoll_event event;
  event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
  event.data.fd = fd;
  return epoll_ctl(poller, EPOLL_CTL_ADD, fd, &event);
}

int directions_poller_wait(int poller, int *fds, int *kinds, int max, int timeout_ms) {
  struct epoll_event events[DIRECTIONS_EVENTS];
  if (max > DIRECTIONS_EVENTS) max = DIRECTIONS_EVENTS;
  int count = epoll_wait(poller, events, max, timeout_ms);
  for (int i = 0; i < count; i++) {
    unsigned int happened = events[i].events;
    int kind = 0;
    if (happened & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) kind |= DIRECTIONS_READABLE;
    if (happened & (EPOLLOUT | EPOLLHUP | EPOLLERR)) kind |= DIRECTIONS_WRITABLE;
    fds[i] = events[i].data.fd;
    kinds[i] = kind;
  }
  return count;
}

#else
#error "directions: no poller for this platform"
#endif
