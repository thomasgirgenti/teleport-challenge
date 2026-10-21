package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

// formatStatus writes a job's status as aligned "Label: value" lines.
// Exit is shown only for jobs that exited on their own, Signal only for killed jobs, and Ended only for jobs that have ended.
func formatStatus(w io.Writer, st *pb.JobStatus) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "ID:\t%s\n", st.GetJobId())
	fmt.Fprintf(tw, "Owner:\t%s\n", st.GetOwner())
	fmt.Fprintf(tw, "Command:\t%+q\n", append([]string{st.GetCommand()}, st.GetArgs()...))
	fmt.Fprintf(tw, "State:\t%s\n", strings.TrimPrefix(st.GetState().String(), "JOB_STATE_"))
	if st.HasExitCode() {
		fmt.Fprintf(tw, "Exit:\t%d\n", st.GetExitCode())
	}
	if st.HasSignal() {
		fmt.Fprintf(tw, "Signal:\t%s\n", st.GetSignal())
	}
	fmt.Fprintf(tw, "Started:\t%s\n", st.GetStartedAt().AsTime().UTC().Format(time.RFC3339))
	if st.HasEndedAt() {
		fmt.Fprintf(tw, "Ended:\t%s\n", st.GetEndedAt().AsTime().UTC().Format(time.RFC3339))
	}
	return tw.Flush()
}
