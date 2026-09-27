# orchid-client

Client side helpers of the Orchid API, used by Ikebana and Keiki.

- `connect`: a channel to Labellum from configuration values (URLs and optional TLS material).
- `informer`: keeps an up to date view of a collection. It lists the collection, watches it from the revision of the list, resumes from the last revision it received when the watch breaks, and lists again when that revision is not available anymore (`OUT_OF_RANGE`). Events are sent on a channel: `Synced` with the whole collection after each list, then `Changed` for every change.

`PodSource` (with a filter) and `NodeSource` are the watchable collections.
