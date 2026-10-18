package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"google.golang.org/grpc/status"

	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

// TODO: Add configuration system.
// Consider a CLI library that supports flags, environment variables, and a config file with reasonable defaults.
const (
	defaultAddr     = "127.0.0.1:50051" // matches workerd, which listens on IPv4 loopback only
	defaultCertFile = "certs/alice.crt"
	defaultKeyFile  = "certs/alice.key"
	defaultCAFile   = "certs/server-ca.crt"
)

// Exit codes returned by Run
const (
	exitOK          = 0
	exitError       = 1   // the command failed, e.g. an RPC error or unreadable cert
	exitUsage       = 2   // bad flags or arguments; nothing was sent to the server
	exitInterrupted = 130 // ctx was cancelled, e.g. by SIGINT; matches the shell convention 128+SIGINT
)

// errUsage reports invalid arguments after their usage has already been printed
var errUsage = errors.New("invalid usage")

// command is a jobctl subcommand.
// Its arguments are checked before connecting, so usage errors never reach the network.
type command struct {
	name    string
	args    string // argument synopsis for usage text
	summary string
	minArgs int
	maxArgs int // -1 for no limit
	run     func(ctx context.Context, c pb.WorkerServiceClient, args []string, stdout io.Writer) error
}

var commands = []command{
	{"start", "[--] <cmd> [args...]", "Start a job; prints the job ID", 1, -1, start},
	{"stop", "<job-id>", "Kill a running job (SIGKILL)", 1, 1, stop},
	{"status", "<job-id>", "Show job status", 1, 1, jobStatus},
	{"logs", "<job-id>", "Stream output from the start, following until the job ends", 1, 1, logs},
}

// Run runs jobctl with args (without the program name) and returns the process exit code.
// Cancelling ctx aborts any in-flight RPC, and Run returns exitInterrupted.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", defaultAddr, "worker server `address`")
	certFile := fs.String("cert", defaultCertFile, "client certificate `file`")
	keyFile := fs.String("key", defaultKeyFile, "client private key `file`")
	caFile := fs.String("ca", defaultCAFile, "server CA certificate `file`")
	fs.Usage = func() { printUsage(fs) }

	if err := fs.Parse(args); err != nil {
		return parseExitCode(err)
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return exitUsage
	}
	cmd, ok := findCommand(fs.Arg(0))
	if !ok {
		fmt.Fprintf(stderr, "jobctl: unknown command %q\n", fs.Arg(0))
		fs.Usage()
		return exitUsage
	}
	cmdArgs, err := cmd.parse(fs.Args()[1:], stderr)
	if err != nil {
		return parseExitCode(err)
	}

	tlsCfg, err := tlsConfig(*certFile, *keyFile, *caFile)
	if err != nil {
		return fail(stderr, err)
	}
	conn, err := dial(*addr, tlsCfg)
	if err != nil {
		return fail(stderr, err)
	}
	defer conn.Close()

	err = cmd.run(ctx, pb.NewWorkerServiceClient(conn), cmdArgs, stdout)
	switch {
	case err == nil:
		return exitOK
	case ctx.Err() != nil:
		// Interrupted by the user; the RPC error only restates that
		return exitInterrupted
	default:
		return fail(stderr, err)
	}
}

// parse checks a subcommand's arguments and returns them with any leading "--" removed.
// On failure, the usage has already been printed.
func (c command) parse(args []string, stderr io.Writer) ([]string, error) {
	fs := flag.NewFlagSet(c.name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintf(stderr, "usage: jobctl [flags] %s %s\n", c.name, c.args) }

	// Parse stops at the first non-flag argument, so `start ls -la` passes "-la" to ls.
	// A command whose first argument starts with "-" needs "--" in front of it.
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if n := fs.NArg(); n < c.minArgs || (c.maxArgs >= 0 && n > c.maxArgs) {
		fs.Usage()
		return nil, errUsage
	}
	return fs.Args(), nil
}

func findCommand(name string) (command, bool) {
	for _, c := range commands {
		if c.name == name {
			return c, true
		}
	}
	return command{}, false
}

func printUsage(fs *flag.FlagSet) {
	w := fs.Output()
	fmt.Fprintf(w, "usage: jobctl [flags] <command> [args]\n\nCommands:\n")
	for _, c := range commands {
		fmt.Fprintf(w, "  %-28s %s\n", c.name+" "+c.args, c.summary)
	}
	fmt.Fprintf(w, "\nFlags:\n")
	fs.PrintDefaults()
}

// parseExitCode returns the exit code for a flag parsing error; -h and --help are not failures
func parseExitCode(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	return exitUsage
}

// fail prints err and returns exitError.
// For a gRPC error only the status message is printed, e.g. "job not found" rather than "rpc error: code = NotFound desc = job not found".
func fail(stderr io.Writer, err error) int {
	msg := err.Error()
	if s, ok := status.FromError(err); ok {
		msg = s.Message()
	}
	fmt.Fprintf(stderr, "error: %s\n", msg)
	return exitError
}
