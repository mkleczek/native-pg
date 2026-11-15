/* The calls to c-ares that native-pg's resolver makes. c-ares calls back into the program, which a Haskell foreign call
   that is unsafe can't take, so the callback is here: it only stores what the lookup gave. Nothing in this file waits:
   c-ares works on sockets that don't block, and poll is given no time. */

/* ares_getsock is marked deprecated in favour of a callback on every change of a socket's state. Asking for the sockets
   when the program is about to wait is simpler, and the function stays in c-ares' API. */
#define CARES_NO_DEPRECATED
#include <ares.h>
#include <netdb.h>
#include <poll.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

/* A lookup that is under way. The callback of c-ares fills it, on whichever thread processes the channel at the time,
   and the part that started it reads it once the state says it is done. */
typedef struct np_lookup {
  ares_channel_t *channel;
  char *name;
  char *service;
  int flags;
  atomic_int done;
  atomic_int abandoned;
  int status;
  struct ares_addrinfo *result;
} np_lookup;

static void np_start(np_lookup *lookup);

static void np_result(void *arg, int status, int timeouts, struct ares_addrinfo *result)
{
  np_lookup *lookup = arg;
  (void)timeouts;
  /* Another lookup's cancelling cancels every query of the channel: this one is asked again, unless its own part
     has given up. c-ares allows a new query from a callback, and a cancel doesn't cancel what its callbacks start. */
  if (status == ARES_ECANCELLED && !atomic_load_explicit(&lookup->abandoned, memory_order_acquire)) {
    np_start(lookup);
    return;
  }
  lookup->status = status;
  lookup->result = result;
  atomic_store_explicit(&lookup->done, 1, memory_order_release);
}

static void np_start(np_lookup *lookup)
{
  struct ares_addrinfo_hints hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_flags = lookup->flags;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  ares_getaddrinfo(lookup->channel, lookup->name, lookup->service, &hints, np_result, lookup);
}

/* A channel, with the system's configuration except where an option replaces it, or NULL and why: a c-ares status, or -1
   if c-ares has no thread support, which a channel that every core uses needs. It also reads the hosts file now, so that
   no lookup has to read it for the first time. */
ares_channel_t *np_cares_new(const char *servers, int timeout_ms, int tries, const char *hosts, int search, int *status)
{
  struct ares_options options;
  ares_channel_t *channel = NULL;
  int mask = ARES_OPT_FLAGS;
  int code;

  if (!ares_threadsafety()) {
    *status = -1;
    return NULL;
  }
  memset(&options, 0, sizeof options);
  /* HOSTALIASES names a file that c-ares would read for every lookup, if the variable is set. */
  options.flags = ARES_FLAG_NOALIASES | (search ? 0 : ARES_FLAG_NOSEARCH);
  if (timeout_ms > 0) {
    options.timeout = timeout_ms;
    mask |= ARES_OPT_TIMEOUTMS;
  }
  if (tries > 0) {
    options.tries = tries;
    mask |= ARES_OPT_TRIES;
  }
  if (hosts) {
    options.hosts_path = (char *)hosts;
    mask |= ARES_OPT_HOSTS_FILE;
  }
  code = ares_library_init(ARES_LIB_INIT_ALL);
  if (code == ARES_SUCCESS) {
    code = ares_init_options(&channel, &options, mask);
  }
  if (code == ARES_SUCCESS && servers) {
    code = ares_set_servers_ports_csv(channel, servers);
  }
  if (code != ARES_SUCCESS) {
    if (channel) ares_destroy(channel);
    *status = code;
    return NULL;
  }
  /* "localhost" is answered from the hosts file, or from c-ares itself without a query. Asking for it reads the file. */
  {
    np_lookup warm;
    memset(&warm, 0, sizeof warm);
    warm.channel = channel;
    warm.name = "localhost";
    warm.service = "0";
    warm.flags = ARES_AI_NUMERICSERV;
    warm.abandoned = 1;
    np_start(&warm);
    ares_cancel(channel);
    if (warm.result) ares_freeaddrinfo(warm.result);
  }
  *status = ARES_SUCCESS;
  return channel;
}

void np_cares_destroy(ares_channel_t *channel)
{
  ares_destroy(channel);
}

const char *np_cares_error(int status)
{
  return ares_strerror(status);
}

/* Starts a lookup of the name and the service, which is a port number. The addresses are sorted as RFC 6724 says, unless
   sort is 0, which leaves them in the order that the server gave them. */
np_lookup *np_cares_lookup(ares_channel_t *channel, const char *name, const char *service, int sort)
{
  np_lookup *lookup = calloc(1, sizeof *lookup);
  if (!lookup) return NULL;
  lookup->channel = channel;
  lookup->flags = ARES_AI_NUMERICSERV | (sort ? 0 : ARES_AI_NOSORT);
  lookup->name = strdup(name);
  lookup->service = strdup(service);
  if (!lookup->name || !lookup->service) {
    free(lookup->name);
    free(lookup->service);
    free(lookup);
    return NULL;
  }
  np_start(lookup);
  return lookup;
}

int np_lookup_done(np_lookup *lookup)
{
  return atomic_load_explicit(&lookup->done, memory_order_acquire);
}

/* The c-ares status of a lookup that is done. */
int np_lookup_status(np_lookup *lookup)
{
  return lookup->status;
}

/* The first address of a lookup that is done, and the one after an address: a sockaddr, with its length. */
const struct ares_addrinfo_node *np_lookup_first(np_lookup *lookup)
{
  return lookup->result ? lookup->result->nodes : NULL;
}

const struct ares_addrinfo_node *np_node_next(const struct ares_addrinfo_node *node)
{
  return node->ai_next;
}

const struct sockaddr *np_node_address(const struct ares_addrinfo_node *node)
{
  return node->ai_addr;
}

/* The end of a lookup, which may still be under way: c-ares is told to give up what it asks, and the lookup is freed. */
void np_lookup_end(np_lookup *lookup)
{
  if (!atomic_load_explicit(&lookup->done, memory_order_acquire)) {
    atomic_store_explicit(&lookup->abandoned, 1, memory_order_release);
    ares_cancel(lookup->channel);
  }
  if (lookup->result) ares_freeaddrinfo(lookup->result);
  free(lookup->name);
  free(lookup->service);
  free(lookup);
}

/* What the channel waits for: up to the capacity of sockets, with 1 for the ones it wants to read, 2 for the ones it
   wants to write and 3 for both; how many; and the milliseconds until it has to be processed again, or -1 if no query
   is waiting. */
int np_cares_wanted(ares_channel_t *channel, int *fds, int *wants, int capacity, int *timeout_ms)
{
  ares_socket_t sockets[ARES_GETSOCK_MAXNUM];
  struct timeval remaining;
  struct timeval *next;
  int bits = ares_getsock(channel, sockets, ARES_GETSOCK_MAXNUM);
  int count = 0;
  for (int i = 0; i < ARES_GETSOCK_MAXNUM && count < capacity; i++) {
    int read = ARES_GETSOCK_READABLE(bits, i) != 0;
    int write = ARES_GETSOCK_WRITABLE(bits, i) != 0;
    if (read || write) {
      fds[count] = (int)sockets[i];
      wants[count] = (read ? 1 : 0) | (write ? 2 : 0);
      count++;
    }
  }
  next = ares_timeout(channel, NULL, &remaining);
  *timeout_ms = next ? (int)(remaining.tv_sec * 1000 + (remaining.tv_usec + 999) / 1000) : -1;
  return count;
}

/* Does what the sockets that are ready, and the queries that have timed out, ask of the channel. A lookup that is done
   has its callback called here. The sockets are asked, with no time to wait, which of them is ready. */
void np_cares_process(ares_channel_t *channel)
{
  ares_socket_t sockets[ARES_GETSOCK_MAXNUM];
  struct pollfd polled[ARES_GETSOCK_MAXNUM];
  ares_fd_events_t ready[ARES_GETSOCK_MAXNUM];
  int bits = ares_getsock(channel, sockets, ARES_GETSOCK_MAXNUM);
  int asked = 0;
  int found = 0;
  for (int i = 0; i < ARES_GETSOCK_MAXNUM; i++) {
    int read = ARES_GETSOCK_READABLE(bits, i) != 0;
    int write = ARES_GETSOCK_WRITABLE(bits, i) != 0;
    if (read || write) {
      polled[asked].fd = sockets[i];
      polled[asked].events = (short)((read ? POLLIN : 0) | (write ? POLLOUT : 0));
      polled[asked].revents = 0;
      asked++;
    }
  }
  if (asked > 0) poll(polled, (nfds_t)asked, 0);
  for (int i = 0; i < asked; i++) {
    unsigned int events = 0;
    if (polled[i].revents & (POLLIN | POLLHUP | POLLERR)) events |= ARES_FD_EVENT_READ;
    if (polled[i].revents & POLLOUT) events |= ARES_FD_EVENT_WRITE;
    if (events) {
      ready[found].fd = polled[i].fd;
      ready[found].events = events;
      found++;
    }
  }
  ares_process_fds(channel, found ? ready : NULL, (size_t)found, ARES_PROCESS_FLAG_NONE);
}
