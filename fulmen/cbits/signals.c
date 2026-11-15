/*
 * Signals as a descriptor that becomes readable: the handler of a signal writes the number of the signal to a pipe, which
 * is all that a handler may safely do, and the read end is what a part of the server waits on, as it waits on a socket, so
 * that a signal is an event of the scheduler's poller and reaches it at once, wherever the scheduler sleeps. Nothing reads
 * the pipe: once a signal has come, the descriptor stays readable.
 */
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

static int fulmen_pipe[2] = {-1, -1};
static volatile sig_atomic_t fulmen_last = 0;

static void fulmen_on_signal(int number) {
  int saved = errno;
  unsigned char byte = (unsigned char)number;
  fulmen_last = number;
  /* A pipe that is full has plenty of signals to wake the waiters. */
  ssize_t ignored = write(fulmen_pipe[1], &byte, 1);
  (void)ignored;
  errno = saved;
}

static int fulmen_set_flags(int fd) {
  int flags = fcntl(fd, F_GETFL);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
  int descriptor_flags = fcntl(fd, F_GETFD);
  if (descriptor_flags < 0 || fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) return -1;
  return 0;
}

/* Has the signals set a handler that writes to the pipe, which is made the first time. Gives the read end of the pipe, or -1. */
int fulmen_signals(const int *numbers, int count) {
  if (fulmen_pipe[0] < 0) {
    if (pipe(fulmen_pipe) < 0) return -1;
    if (fulmen_set_flags(fulmen_pipe[0]) < 0 || fulmen_set_flags(fulmen_pipe[1]) < 0) {
      close(fulmen_pipe[0]);
      close(fulmen_pipe[1]);
      fulmen_pipe[0] = fulmen_pipe[1] = -1;
      return -1;
    }
  }
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_handler = fulmen_on_signal;
  sigemptyset(&action.sa_mask);
  action.sa_flags = SA_RESTART;
  for (int i = 0; i < count; i++)
    if (sigaction(numbers[i], &action, NULL) < 0) return -1;
  return fulmen_pipe[0];
}

/* The number of the signal that came last, or 0. */
int fulmen_last_signal(void) { return (int)fulmen_last; }
