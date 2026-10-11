#ifndef NATIVE_PG_TLS_H
#define NATIVE_PG_TLS_H

#include <rustls.h>

rustls_io_result native_pg_tls_read(struct rustls_connection *connection, int socket, size_t *received);
rustls_io_result native_pg_tls_write(struct rustls_connection *connection, int socket, size_t *sent);
rustls_result native_pg_tls_accept_any_certificate(struct rustls_client_config_builder *builder);
rustls_result native_pg_tls_offer_postgresql(struct rustls_client_config_builder *builder);

#endif
