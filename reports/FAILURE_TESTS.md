# Failure-injection results

Run 2026-11-11 03:57 UTC. Each scenario runs the workload generator, injects the fault, lets the system recover, then requires an exact reconciliation with Postgres.

| scenario | fault injected | recovery | reconciled | events in lake | duplicate changes absorbed |
|---|---|---|---|---|---|
| Spark driver killed | SIGKILL of the Spark app mid-stream, down 10 s | restarted from checkpoints, first batch after 19s | yes (17s after load stopped) | 504932 | 250 |
| crash before scd2 | driver exits at `before_scd2` inside a batch | same batch id replayed on restart | yes (16s after load stopped) | 510136 | 250 |
| crash after silver | driver exits at `after_silver` inside a batch | same batch id replayed on restart | yes (18s after load stopped) | 515134 | 250 |
| crash after quarantine | driver exits at `after_quarantine` inside a batch | same batch id replayed on restart | yes (10s after load stopped) | 520035 | 250 |
| Kafka broker killed | SIGKILL of the broker, down 25 s | broker restarted; Debezium RUNNING after 8s; Spark Kafka source retried in place | yes (18s after load stopped) | 527750 | 250 |
| Kafka Connect killed | SIGKILL of Connect/Debezium, down 15 s | connector resumed from stored offsets, RUNNING after 0s | yes (18s after load stopped) | 534437 | 313 |
| Postgres crashed | SIGKILL of Postgres between two load phases, down 10 s | crash recovery; Debezium state RUNNING/RUNNING after 0s | yes (18s after load stopped) | 539863 | 313 |
