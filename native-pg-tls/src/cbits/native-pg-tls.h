#ifndef NATIVE_PG_TLS_H
#define NATIVE_PG_TLS_H

#include <rustls.h>

rustls_io_result native_pg_tls_read(struct rustls_connection *connection, int socket, size_t *received);
rustls_io_result native_pg_tls_write(struct rustls_connection *connection, int socket, size_t *sent);

/*
 * native_pg_tls_receive gives the bytes it read into the buffer, 0 at the end
 * of the stream, or one of these. An error is -(kind << 32 | value).
 */
#define NATIVE_PG_TLS_RECEIVE_WOULD_BLOCK (-1)
#define NATIVE_PG_TLS_RECEIVE_AGAIN (-2)
#define NATIVE_PG_TLS_RECEIVE_READ_FAILED 1    /* value: rustls_connection_read's result */
#define NATIVE_PG_TLS_RECEIVE_PACKETS_FAILED 2 /* value: rustls_connection_process_new_packets's result */
#define NATIVE_PG_TLS_RECEIVE_RECV_FAILED 3    /* value: errno */
int64_t native_pg_tls_receive(struct rustls_connection *connection, int socket, uint8_t *buffer, size_t size);
rustls_result native_pg_tls_accept_any_certificate(struct rustls_client_config_builder *builder);
rustls_result native_pg_tls_offer_postgresql(struct rustls_client_config_builder *builder);

#endif
