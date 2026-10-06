# Teleport Core
Worker library with methods to start/stop/query status of a job.

Library should be able to stream the output of a job.
* Discovering new output should be efficient, avoid busy-waiting or polling.
* Output should be from start of process execution.
* Multiple concurrent clients should be supported.
* Do not make any assumptions about the process's output - it may be text or raw binary data.