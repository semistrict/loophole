package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ergochat/readline"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/sqlitevfs"
)

func dbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "SQLite database commands",
	}

	cmd.AddCommand(
		dbCreateCmd(),
		dbSnapshotCmd(),
		dbBranchCmd(),
		dbFlushCmd(),
		dbLsCmd(),
		dbSQLCmd(),
		dbFollowCmd(),
	)

	return cmd
}

func dbCreateCmd() *cobra.Command {
	var sizeStr string

	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new SQLite database volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}

			var size uint64
			if sizeStr != "" {
				size, err = parseSize(sizeStr)
				if err != nil {
					return err
				}
			}

			if err := c.DBCreate(cmd.Context(), client.DBCreateParams{
				Volume: args[0],
				Size:   size,
			}); err != nil {
				return err
			}
			fmt.Printf("created database %s\n", args[0])
			return nil
		},
	}

	cmd.Flags().StringVarP(&sizeStr, "size", "s", "", "volume size (e.g. 1GB, default 1GB)")
	return cmd
}

func dbSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot <name> <snapshot-name>",
		Short: "Snapshot a database (always flushes first)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			if err := c.DBSnapshot(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			fmt.Printf("snapshot %q of database %s created\n", args[1], args[0])
			return nil
		},
	}
}

func dbBranchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "branch <name> <branch-name>",
		Short: "Create a writable branch of a database",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			if err := c.DBBranch(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			fmt.Printf("branched database %s as %s\n", args[0], args[1])
			return nil
		},
	}
}

func dbFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush <name>",
		Short: "Explicitly flush database to S3",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			if err := c.DBFlush(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Printf("flushed database %s\n", args[0])
			return nil
		},
	}
}

func dbLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List database volumes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			volumes, err := c.DBList(cmd.Context())
			if err != nil {
				return err
			}
			for _, name := range volumes {
				fmt.Println(name)
			}
			return nil
		},
	}
}

func dbSQLCmd() *cobra.Command {
	var async bool
	var readOnly bool

	cmd := &cobra.Command{
		Use:   "sql <name>",
		Short: "Interactive SQLite REPL",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}

			nbdSock, err := c.NBDSock(cmd.Context())
			if err != nil {
				return err
			}

			cfg := &replConfig{
				client:   c,
				nbdSock:  nbdSock,
				syncMode: syncModeFrom(async),
				readOnly: readOnly,
				ctx:      cmd.Context(),
			}
			if err := cfg.openDB(args[0]); err != nil {
				return err
			}
			defer cfg.cleanup()

			return runREPL(cfg)
		},
	}

	cmd.Flags().BoolVar(&async, "async", false, "use async sync mode (lower latency, weaker durability)")
	cmd.Flags().BoolVar(&readOnly, "read-only", false, "open in read-only mode")
	return cmd
}

func dbFollowCmd() *cobra.Command {
	var interval time.Duration

	cmd := &cobra.Command{
		Use:   "follow <name>",
		Short: "Follow a live database (read-only REPL with periodic refresh)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}

			nbdSock, err := c.NBDSock(cmd.Context())
			if err != nil {
				return err
			}

			vol, err := sqlitevfs.DialNBD(cmd.Context(), nbdSock, args[0])
			if err != nil {
				return err
			}
			defer func() { _ = vol.ReleaseRef(cmd.Context()) }()

			dbVFS, err := sqlitevfs.NewVolumeVFS(cmd.Context(), vol, sqlitevfs.SyncModeSync)
			if err != nil {
				return err
			}

			// Start a goroutine that periodically re-reads the superblock
			// to see new data flushed by the writer.
			stopRefresh := make(chan struct{})
			defer close(stopRefresh)
			go func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						if sb, err := sqlitevfs.ReadSuperblock(cmd.Context(), vol); err == nil {
							dbVFS.SetSuperblock(sb)
						}
					case <-stopRefresh:
						return
					}
				}
			}()

			vfsName := "loophole-follow-" + args[0]
			if err := sqlite3vfs.RegisterVFS(vfsName, dbVFS); err != nil {
				return fmt.Errorf("register VFS: %w", err)
			}

			sqlDB, err := sql.Open("sqlite3", "file:main.db?vfs="+vfsName+"&mode=ro")
			if err != nil {
				return fmt.Errorf("open SQLite: %w", err)
			}
			defer util.SafeClose(sqlDB, "close follow db")

			fmt.Printf("following %s (refreshing every %s, read-only)\n", args[0], interval)
			return runREPL(&replConfig{db: sqlDB, name: args[0], ctx: cmd.Context()})
		},
	}

	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "refresh interval")
	return cmd
}

func syncModeFrom(async bool) sqlitevfs.SyncMode {
	if async {
		return sqlitevfs.SyncModeAsync
	}
	return sqlitevfs.SyncModeSync
}

// sqlCompleter provides tab completion for the SQL REPL.
type sqlCompleter struct {
	db *sql.DB
}

func (c *sqlCompleter) Do(line []rune, pos int) ([][]rune, int) {
	// Get the word being typed (go backwards from cursor to find start).
	start := pos
	for start > 0 && line[start-1] != ' ' && line[start-1] != ',' && line[start-1] != '(' {
		start--
	}
	prefix := strings.ToUpper(string(line[start:pos]))
	if prefix == "" {
		return nil, 0
	}

	var candidates []string

	// Dot-commands.
	if start == 0 && strings.HasPrefix(".", string(line[start:pos])) {
		for _, cmd := range []string{".tables", ".schema", ".list", ".snapshot", ".branch", ".use", ".quit", ".exit"} {
			if strings.HasPrefix(cmd, strings.ToLower(prefix)) {
				candidates = append(candidates, cmd[pos-start:])
			}
		}
		return toRunes(candidates), pos - start
	}

	// SQL keywords.
	for _, kw := range sqlKeywords {
		if strings.HasPrefix(kw, prefix) {
			candidates = append(candidates, kw[len(prefix):])
		}
	}

	// Table names (case-insensitive match against prefix).
	if c.db != nil {
		rows, err := c.db.Query("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND UPPER(name) LIKE ? ORDER BY name", prefix+"%")
		if err == nil {
			defer util.SafeClose(rows, "close rows")
			for rows.Next() {
				var tbl string
				if rows.Scan(&tbl) == nil {
					// Only add if not already a keyword match.
					upper := strings.ToUpper(tbl)
					if !strings.HasPrefix(upper, prefix) {
						continue
					}
					candidates = append(candidates, tbl[len(prefix):]+" ")
				}
			}
		}
	}

	return toRunes(candidates), len(prefix)
}

func toRunes(ss []string) [][]rune {
	out := make([][]rune, len(ss))
	for i, s := range ss {
		out[i] = []rune(s)
	}
	return out
}

var sqlKeywords = []string{
	"ALTER", "AND", "AS", "ASC",
	"BEGIN", "BETWEEN", "BY",
	"CASE", "CAST", "CHECK", "COLUMN", "COMMIT", "CONSTRAINT", "CREATE", "CROSS",
	"DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP",
	"ELSE", "END", "EXCEPT", "EXISTS", "EXPLAIN",
	"FOREIGN", "FROM", "FULL",
	"GROUP",
	"HAVING",
	"IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS",
	"JOIN",
	"KEY",
	"LEFT", "LIKE", "LIMIT",
	"NOT", "NULL",
	"OFFSET", "ON", "OR", "ORDER", "OUTER",
	"PRAGMA", "PRIMARY",
	"REFERENCES", "REPLACE", "RIGHT", "ROLLBACK", "ROW",
	"SELECT", "SET",
	"TABLE", "THEN", "TRANSACTION", "TRIGGER",
	"UNION", "UNIQUE", "UPDATE", "USING",
	"VALUES", "VIEW",
	"WHEN", "WHERE", "WITH",
}

type replConfig struct {
	db       *sql.DB
	name     string
	client   *client.Client // nil in read-only/follow mode
	nbdSock  string         // for .use reconnection
	syncMode sqlitevfs.SyncMode
	readOnly bool
	ctx      context.Context

	// cleanup closes current db/vol; set by openDB, called by .use and on exit.
	vol     *sqlitevfs.NBDVolume
	vfsSeq  int
	cleanup func()
}

// openDB connects to a volume over NBD and opens a SQLite database on it.
// It sets cfg.db, cfg.vol, cfg.name, and cfg.cleanup.
func (cfg *replConfig) openDB(name string) error {
	vol, err := sqlitevfs.DialNBD(cfg.ctx, cfg.nbdSock, name)
	if err != nil {
		return err
	}

	dbVFS, err := sqlitevfs.NewVolumeVFS(cfg.ctx, vol, cfg.syncMode)
	if err != nil {
		_ = vol.ReleaseRef(cfg.ctx)
		return err
	}

	cfg.vfsSeq++
	vfsName := fmt.Sprintf("loophole-repl-%s-%d", name, cfg.vfsSeq)
	if err := sqlite3vfs.RegisterVFS(vfsName, dbVFS); err != nil {
		_ = vol.ReleaseRef(cfg.ctx)
		return fmt.Errorf("register VFS: %w", err)
	}

	uri := "file:main.db?vfs=" + vfsName
	if cfg.readOnly {
		uri += "&mode=ro"
	}
	sqlDB, err := sql.Open("sqlite3", uri)
	if err != nil {
		_ = vol.ReleaseRef(cfg.ctx)
		return fmt.Errorf("open SQLite: %w", err)
	}

	cfg.db = sqlDB
	cfg.vol = vol
	cfg.name = name
	cfg.cleanup = func() {
		util.SafeClose(sqlDB, "close repl db")
		_ = vol.ReleaseRef(cfg.ctx)
	}
	return nil
}

func runREPL(cfg *replConfig) error {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:            "sql> ",
		HistoryFile:       os.ExpandEnv("$HOME/.loophole/sql_history"),
		InterruptPrompt:   "^C",
		EOFPrompt:         ".quit",
		HistorySearchFold: true,
		AutoComplete:      &sqlCompleter{db: cfg.db},
	})
	if err != nil {
		return fmt.Errorf("init readline: %w", err)
	}
	defer util.SafeClose(rl, "close readline")

	printBanner := func() {
		fmt.Printf("loophole db: %s\n", cfg.name)
		fmt.Println("Type .quit to exit, .tables to list tables")
		if cfg.client != nil {
			fmt.Println("     .list [prefix], .snapshot <name>, .branch <name>, .use <name>")
		}
	}
	printBanner()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == ".quit" || line == ".exit" {
			break
		}
		if line == ".tables" {
			line = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
		}
		if line == ".schema" {
			line = "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
		}

		// Dot-commands: list, snapshot, branch, use.
		if line == ".list" || strings.HasPrefix(line, ".list ") {
			if cfg.client == nil {
				fmt.Fprintf(os.Stderr, "Error: .list not available in this mode\n")
				continue
			}
			prefix := strings.TrimSpace(strings.TrimPrefix(line, ".list"))
			volumes, err := cfg.client.DBList(cfg.ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
				continue
			}
			for _, name := range volumes {
				if prefix == "" || strings.HasPrefix(name, prefix) {
					if name == cfg.name {
						fmt.Printf("* %s\n", name)
					} else {
						fmt.Printf("  %s\n", name)
					}
				}
			}
			continue
		}
		if strings.HasPrefix(line, ".snapshot ") {
			snapName := strings.TrimSpace(line[len(".snapshot "):])
			if snapName == "" {
				fmt.Fprintf(os.Stderr, "Usage: .snapshot <name>\n")
				continue
			}
			if cfg.client == nil {
				fmt.Fprintf(os.Stderr, "Error: snapshots not available in read-only mode\n")
				continue
			}
			if err := cfg.client.DBSnapshot(cfg.ctx, cfg.name, snapName); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			} else {
				fmt.Printf("snapshot %q created\n", snapName)
			}
			continue
		}
		if strings.HasPrefix(line, ".branch ") {
			branchName := strings.TrimSpace(line[len(".branch "):])
			if branchName == "" {
				fmt.Fprintf(os.Stderr, "Usage: .branch <name>\n")
				continue
			}
			if cfg.client == nil {
				fmt.Fprintf(os.Stderr, "Error: branching not available in read-only mode\n")
				continue
			}
			if err := cfg.client.DBBranch(cfg.ctx, cfg.name, branchName); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			} else {
				fmt.Printf("branch %q created\n", branchName)
			}
			continue
		}
		if strings.HasPrefix(line, ".use ") {
			newName := strings.TrimSpace(line[len(".use "):])
			if newName == "" {
				fmt.Fprintf(os.Stderr, "Usage: .use <name>\n")
				continue
			}
			if cfg.nbdSock == "" {
				fmt.Fprintf(os.Stderr, "Error: .use not available in this mode\n")
				continue
			}
			// Close current connection and open new volume.
			if cfg.cleanup != nil {
				cfg.cleanup()
			}
			if err := cfg.openDB(newName); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
				// Try to reconnect to the previous volume.
				fmt.Fprintf(os.Stderr, "Reconnecting to previous volume is not possible; exiting.\n")
				return err
			}
			rl.SetPrompt("sql> ")
			printBanner()
			continue
		}

		if isQuery(line) {
			if err := execQuery(cfg.db, line); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			}
		} else {
			result, err := cfg.db.Exec(line)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
				continue
			}
			affected, _ := result.RowsAffected()
			if affected > 0 {
				fmt.Printf("%d row(s) affected\n", affected)
			}
		}
	}
	return nil
}

func isQuery(line string) bool {
	upper := strings.ToUpper(strings.TrimSpace(line))
	return strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "WITH")
}

func execQuery(db *sql.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer util.SafeClose(rows, "close query rows")

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, strings.Join(cols, "\t"))
	_, _ = fmt.Fprintln(w, strings.Repeat("---\t", len(cols)))

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprintf("%v", v)
			}
		}
		_, _ = fmt.Fprintln(w, strings.Join(parts, "\t"))
		count++
	}
	_ = w.Flush()
	if err := rows.Err(); err != nil {
		return err
	}
	fmt.Printf("(%d row(s))\n", count)
	return nil
}
