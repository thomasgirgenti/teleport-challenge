# Teleport Core
Worker library with methods to start/stop/query status of a job.

## Usage

```go
m := core.NewManager()
defer m.Shutdown(ctx)

job, err := m.Start("alice", "bash", []string{"-c", "echo hello"})
if err != nil {
	return err
}

// Stream output from the first byte until the job ends
io.Copy(os.Stdout, job.Output(ctx))

fmt.Println(job.Status().State) // 2
```

`Job.Stop(ctx)` sends SIGKILL to the job's whole process group.
`Manager.Shutdown(ctx)` rejects new jobs and kills all running jobs.

## Testing

The library targets Linux (it uses process groups and `waitid`). Run the race tests:

```sh
make test         # on Linux
make test-linux   # on other platforms, in a golang:1.27 container w/ Docker
```
