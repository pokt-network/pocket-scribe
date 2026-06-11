package doctor

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/config"
	psnats "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
)

const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"
const defaultNATSURL = natsgo.DefaultURL

// NewCmd builds the `ps doctor` command.
func NewCmd() *cobra.Command {
	var (
		cfgPath string
		dsn     string
		natsURL string
	)
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Health check: Postgres, NATS, LCD, upgrades table",
		Long: `Runs independent health checks and prints a ✓/✗ report.
Exit code 0 = all checks passed. Exit code 1 = at least one check failed.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			results := runChecks(ctx, cfgPath, dsn, natsURL)
			out, code := RenderChecks(results)
			fmt.Print(out)
			if code != 0 {
				// Return a sentinel so cobra doesn't print usage, but the
				// exit code is handled by cmd/ps/main.go os.Exit path.
				os.Exit(code)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "",
		"path to network config YAML (optional; enables LCD check)")
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN; overrides $PS_DATABASE_DSN")
	cmd.Flags().StringVar(&natsURL, "nats-url", envOr("PS_NATS_URL", defaultNATSURL),
		"NATS server URL; overrides $PS_NATS_URL")
	return cmd
}

// runChecks executes all health checks independently (one failure does not
// abort the rest) and returns the results.
func runChecks(ctx context.Context, cfgPath, dsn, natsURL string) []CheckResult {
	var results []CheckResult

	// (a) Postgres reachable + migration version
	results = append(results, checkPostgres(ctx, dsn))

	// (b) NATS reachable + JetStream enabled
	results = append(results, checkNATS(ctx, natsURL))

	// (c) LCD reachable (only when config provided)
	if cfgPath != "" {
		results = append(results, checkLCD(cfgPath))
	}

	// (d) upgrades table non-empty (requires Postgres)
	results = append(results, checkUpgrades(ctx, dsn))

	return results
}

func checkPostgres(ctx context.Context, dsn string) CheckResult {
	st, err := store.New(ctx, dsn)
	if err != nil {
		return CheckResult{Name: "postgres", OK: false, Err: err}
	}
	defer st.Close()

	// Read the current goose migration version.
	var version int64
	err = st.Pool().QueryRow(ctx,
		`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied = true`,
	).Scan(&version)
	if err != nil {
		return CheckResult{Name: "postgres", OK: false, Err: fmt.Errorf("read migration version: %w", err)}
	}
	return CheckResult{
		Name:   "postgres",
		OK:     true,
		Detail: fmt.Sprintf("migration v%d", version),
	}
}

func checkNATS(ctx context.Context, natsURL string) CheckResult {
	// Use a short deadline for the initial connect; the NATS client normally
	// retries indefinitely but here we want a fast fail for the healthcheck.
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client, err := psnats.Connect(dialCtx, natsURL)
	if err != nil {
		return CheckResult{Name: "nats", OK: false, Err: err}
	}
	defer client.Close()

	// Verify JetStream is enabled by listing stream names (fast, read-only).
	js := client.JetStream()
	names := js.StreamNames(dialCtx)
	count := 0
	for range names.Name() {
		count++
	}
	if err := names.Err(); err != nil {
		return CheckResult{Name: "nats", OK: false, Err: fmt.Errorf("JetStream list streams: %w", err)}
	}
	return CheckResult{
		Name:   "nats",
		OK:     true,
		Detail: fmt.Sprintf("JetStream enabled, %d stream(s)", count),
	}
}

func checkLCD(cfgPath string) CheckResult {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return CheckResult{Name: "lcd", OK: false, Err: fmt.Errorf("load config: %w", err)}
	}
	if len(cfg.Endpoints.LCD) == 0 {
		return CheckResult{Name: "lcd", OK: false, Err: fmt.Errorf("no LCD endpoints in config")}
	}
	url := cfg.Endpoints.LCD[0]

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url) //nolint:noctx // deliberate: own timeout via client
	if err != nil {
		return CheckResult{Name: "lcd", OK: false, Err: fmt.Errorf("%s: %w", url, err)}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return CheckResult{
			Name: "lcd",
			OK:   false,
			Err:  fmt.Errorf("%s returned HTTP %d", url, resp.StatusCode),
		}
	}
	return CheckResult{
		Name:   "lcd",
		OK:     true,
		Detail: fmt.Sprintf("%s  HTTP %d", url, resp.StatusCode),
	}
}

func checkUpgrades(ctx context.Context, dsn string) CheckResult {
	st, err := store.New(ctx, dsn)
	if err != nil {
		return CheckResult{Name: "upgrades", OK: false, Err: fmt.Errorf("open store: %w", err)}
	}
	defer st.Close()

	upgrades, err := st.ListUpgrades(ctx)
	if err != nil {
		return CheckResult{Name: "upgrades", OK: false, Err: err}
	}
	if len(upgrades) == 0 {
		return CheckResult{
			Name: "upgrades",
			OK:   false,
			Err:  fmt.Errorf("upgrades table is empty — run `ps sync-upgrades` first"),
		}
	}
	return CheckResult{
		Name:   "upgrades",
		OK:     true,
		Detail: fmt.Sprintf("%d upgrade(s) recorded", len(upgrades)),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
