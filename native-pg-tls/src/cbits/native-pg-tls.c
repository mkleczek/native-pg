/*
 * rustls reads and writes the socket through these callbacks, given its file
 * descriptor, so that no call goes back into Haskell. Sockets don't block on
 * Unix: an empty or full one gives EAGAIN, which rustls passes back.
 */

/* glibc's <limits.h> has IOV_MAX only with X/Open's extensions. */
#define _GNU_SOURCE

#include "native-pg-tls.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#endif

static rustls_io_result receive(void *userdata, uint8_t *buffer, size_t size, size_t *received)
{
#ifdef _WIN32
  int result = recv((SOCKET)(intptr_t)userdata, (char *)buffer, size > INT_MAX ? INT_MAX : (int)size, 0);
  if (result == SOCKET_ERROR)
    return WSAGetLastError();
#else
  ssize_t result = recv((int)(intptr_t)userdata, buffer, size, 0);
  if (result < 0)
    return errno;
#endif
  *received = (size_t)result;
  return 0;
}

static rustls_io_result send_vectored(void *userdata, const struct rustls_iovec *iovecs, size_t count, size_t *sent)
{
#ifdef _WIN32
  DWORD result;
  if (WSASend((SOCKET)(intptr_t)userdata, (LPWSABUF)iovecs, (DWORD)count, &result, 0, NULL, NULL) == SOCKET_ERROR)
    return WSAGetLastError();
#else
  ssize_t result = writev((int)(intptr_t)userdata, (const struct iovec *)iovecs, count > IOV_MAX ? IOV_MAX : (int)count);
  if (result < 0)
    return errno;
#endif
  *sent = (size_t)result;
  return 0;
}

/* Reads what the socket has into the connection's buffers. */
rustls_io_result native_pg_tls_read(struct rustls_connection *connection, int socket, size_t *received)
{
  return rustls_connection_read_tls(connection, receive, (void *)(intptr_t)socket, received);
}

#define RECEIVE_FAILED(kind, value) (-(int64_t)(((uint64_t)(kind) << 32) | (uint32_t)(value)))

/* How many times it reads the socket before it gives AGAIN. */
#define RECEIVE_ROUNDS 16

/*
 * Reads what the connection has into the buffer, up to its size, and what the
 * socket has into the connection first if it has none, all in one call.
 * Gives the bytes read, 0 at the end of the stream, or WOULD_BLOCK, AGAIN
 * after RECEIVE_ROUNDS reads that gave records without plaintext, to be called
 * again, or an error.
 */
int64_t native_pg_tls_receive(struct rustls_connection *connection, int socket, uint8_t *buffer, size_t size)
{
  int ended = 0, rounds = 0;
  for (;;) {
    size_t count = 0;
    rustls_result result = rustls_connection_read(connection, buffer, size, &count);
    if (result == RUSTLS_RESULT_OK)
      return (int64_t)count;
    if (result == RUSTLS_RESULT_UNEXPECTED_EOF || (result == RUSTLS_RESULT_PLAINTEXT_EMPTY && ended))
      return 0;
    if (result != RUSTLS_RESULT_PLAINTEXT_EMPTY)
      return RECEIVE_FAILED(NATIVE_PG_TLS_RECEIVE_READ_FAILED, result);
    rustls_io_result error;
    count = 0;
    while ((error = native_pg_tls_read(connection, socket, &count)) == EINTR)
      ;
    if (error == EAGAIN || error == EWOULDBLOCK)
      return NATIVE_PG_TLS_RECEIVE_WOULD_BLOCK;
    if (error != 0)
      return RECEIVE_FAILED(NATIVE_PG_TLS_RECEIVE_RECV_FAILED, error);
    if (count == 0) {
      ended = 1;
      continue;
    }
    ended = 0;
    result = rustls_connection_process_new_packets(connection);
    if (result != RUSTLS_RESULT_OK)
      return RECEIVE_FAILED(NATIVE_PG_TLS_RECEIVE_PACKETS_FAILED, result);
    if (++rounds == RECEIVE_ROUNDS)
      return NATIVE_PG_TLS_RECEIVE_AGAIN;
  }
}

/* Writes what the socket takes of the connection's records. */
rustls_io_result native_pg_tls_write(struct rustls_connection *connection, int socket, size_t *sent)
{
  return rustls_connection_write_tls_vectored(connection, send_vectored, (void *)(intptr_t)socket, sent);
}

static uint32_t accept_any(void *userdata, const struct rustls_verify_server_cert_params *params)
{
  (void)userdata;
  (void)params;
  return RUSTLS_RESULT_OK;
}

/*
 * Accepts any certificate the server has, as libpq's sslmode require does.
 * rustls still checks that the server holds the certificate's key.
 */
rustls_result native_pg_tls_accept_any_certificate(struct rustls_client_config_builder *builder)
{
  return rustls_client_config_builder_dangerous_set_certificate_verifier(builder, accept_any);
}

/* Offers ALPN's "postgresql", as libpq does since PostgreSQL 17. */
rustls_result native_pg_tls_offer_postgresql(struct rustls_client_config_builder *builder)
{
  static const uint8_t postgresql[] = "postgresql";
  struct rustls_slice_bytes protocol = {postgresql, sizeof postgresql - 1};
  return rustls_client_config_builder_set_alpn_protocols(builder, &protocol, 1);
}
