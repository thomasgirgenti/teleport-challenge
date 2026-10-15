// Package core runs arbitrary Linux processes as jobs.
//
// A Manager starts jobs and looks them up by ID.
// Each Job records its lifecycle state and captures the combined stdout and stderr of its process in memory.
// Any number of readers can stream that output from the first byte, blocking until new output is written or the job ends.
//
// Every job runs in its own process group, so Stop kills the job's whole process tree with SIGKILL.
package core
