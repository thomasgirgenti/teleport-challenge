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
	fmt.Fprintf(tw, "Command:\t%s\n", shellQuote(append([]string{st.GetCommand()}, st.GetArgs()...)))
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

// shellQuote joins args into a single line. Args with characters the shell treats specially are single-quoted.
func shellQuote(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = quoteArg(arg)
	}
	return strings.Join(quoted, " ")
}

func quoteArg(s string) string {
	if s != "" && strings.IndexFunc(s, isUnsafe) < 0 {
		return s
	}
	// Inside single quotes nothing is special except ', which is written as '\'' (close, escaped quote, reopen)
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// isUnsafe reports whether r needs quoting to be read literally by a shell
func isUnsafe(r rune) bool {
	switch {
	case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
		return false
	}
	return !strings.ContainsRune("@%+=:,./_-", r)
}
