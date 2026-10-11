/*
 * rustls reads and writes the socket through these callbacks, given its file
 * descriptor, so that no call goes back into Haskell. Sockets don't block on
 * Unix: an empty or full one gives EAGAIN, which rustls passes back.
 */

/* glibc's <limits.h> has IOV_MAX only with X/Open's extensions. */
#define _GNU_SOURCE

#include "native-pg-tls.h"

#include <limits.h>
#include <stdint.h>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <errno.h>
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
