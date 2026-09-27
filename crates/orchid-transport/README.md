# orchid-transport

gRPC transport shared by the Orchid services. A cluster runs either in cleartext or in mTLS mode, the mode of a component is chosen by the presence of a `[tls]` section in its configuration.

- `url`: parses service URLs and checks them against the mode (`http://` in cleartext, `https://` in mTLS, `unix://` in both) and listen addresses (`tcp://ip:port`, `unix://path`).
- `tls`: loads the certificate, key and CA of a component.
- `client`: lazy channels to one or several URLs (balanced), over TCP or a unix socket.
- `server`: servers requiring client certificates in mTLS mode, listening on several addresses.
- `identity`: identity of a caller from the Common Name of its certificate (`labellum`, `ikebana`, `keiki:<node>`, `user:<name>`), `Anonymous` in cleartext mode.
- `errors`: machine readable reasons attached to statuses (`google.rpc.ErrorInfo`), e.g. the `Bind` failures.

In mTLS mode over a unix socket, clients check the server certificate against `localhost`: the Labellum certificate needs a `localhost` SAN.

The `testing` feature adds `TestPki`, which issues throwaway certificates for tests.
