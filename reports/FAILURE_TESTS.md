# Failure-injection results

Run 2026-11-11 02:22 UTC. Each scenario runs the workload generator, injects the fault, lets the system recover, then requires an exact reconciliation with Postgres.

| scenario | fault injected | recovery | reconciled | events in lake | duplicate changes absorbed |
|---|---|---|---|---|---|
| Spark driver killed | SIGKILL of the Spark app mid-stream, down 10 s | restarted from checkpoints, first batch after 21s | yes (15s after load stopped) | 249723 | 67 |
| crash before scd2 | driver exits at `before_scd2` inside a batch | same batch id replayed on restart | yes (15s after load stopped) | 254784 | 67 |
| crash after silver | driver exits at `after_silver` inside a batch | same batch id replayed on restart | yes (16s after load stopped) | 259781 | 67 |
| crash after quarantine | driver exits at `after_quarantine` inside a batch | same batch id replayed on restart | yes (16s after load stopped) | 264841 | 67 |
| Kafka broker killed | SIGKILL of the broker, down 25 s | broker restarted; Debezium RUNNING after 8s; Spark Kafka source retried in place | yes (15s after load stopped) | 272724 | 67 |
| Kafka Connect killed | SIGKILL of Connect/Debezium, down 15 s | connector resumed from stored offsets, RUNNING after 0s | yes (16s after load stopped) | 279547 | 250 |
| Postgres crashed | SIGKILL of Postgres between two load phases, down 10 s | crash recovery; Debezium state RUNNING/RUNNING after 0s | yes (16s after load stopped) | 285031 | 250 |
