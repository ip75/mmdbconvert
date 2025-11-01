// mmdbconvert merges multiple MaxMind MMDB databases and exports to CSV or Parquet format.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/writer"
)

const version = "0.1.0"

func main() {
	// Define command-line flags
	var (
		configPath string
		quiet      bool
		showHelp   bool
		showVer    bool
	)

	flag.StringVar(&configPath, "config", "", "Path to TOML configuration file")
	flag.BoolVar(&quiet, "quiet", false, "Suppress progress output")
	flag.BoolVar(&showHelp, "help", false, "Show usage information")
	flag.BoolVar(&showVer, "version", false, "Show version information")

	flag.Usage = usage
	flag.Parse()

	// Handle version flag
	if showVer {
		fmt.Printf("mmdbconvert version %s\n", version)
		os.Exit(0)
	}

	// Handle help flag
	if showHelp {
		usage()
		os.Exit(0)
	}

	// Get config path from positional argument if not specified with flag
	if configPath == "" {
		if flag.NArg() == 0 {
			fmt.Fprint(os.Stderr, "Error: config file path required\n\n")
			usage()
			os.Exit(1)
		}
		configPath = flag.Arg(0)
	}

	// Run the conversion
	if err := run(configPath, quiet); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// run performs the main conversion process.
func run(configPath string, quiet bool) error {
	startTime := time.Now()

	if !quiet {
		fmt.Printf("mmdbconvert v%s\n", version)
		fmt.Printf("Loading configuration from %s...\n", configPath)
	}

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if !quiet {
		fmt.Printf("Output format: %s\n", cfg.Output.Format)
		fmt.Printf("Output file: %s\n", cfg.Output.File)
		fmt.Printf("Databases: %d\n", len(cfg.Databases))
		fmt.Printf("Data columns: %d\n", len(cfg.Columns))
		fmt.Printf("Network columns: %d\n", len(cfg.Network.Columns))
		fmt.Println()
	}

	// Open MMDB databases
	if !quiet {
		fmt.Println("Opening MMDB databases...")
	}

	databases := map[string]string{}
	for _, db := range cfg.Databases {
		databases[db.Name] = db.Path
		if !quiet {
			fmt.Printf("  - %s: %s\n", db.Name, db.Path)
		}
	}

	readers, err := mmdb.OpenDatabases(databases)
	if err != nil {
		return fmt.Errorf("failed to open databases: %w", err)
	}
	defer readers.Close()

	if !quiet {
		fmt.Println()
		fmt.Println("Creating output file...")
	}

	// Create output file
	outputFile, err := os.Create(cfg.Output.File)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// Create writer based on format
	var rowWriter merger.RowWriter
	switch cfg.Output.Format {
	case "csv":
		rowWriter = writer.NewCSVWriter(outputFile, cfg)
	case "parquet":
		parquetWriter, err := writer.NewParquetWriter(outputFile, cfg)
		if err != nil {
			return fmt.Errorf("failed to create Parquet writer: %w", err)
		}
		rowWriter = parquetWriter
	default:
		return fmt.Errorf("unsupported output format: %s", cfg.Output.Format)
	}

	if !quiet {
		fmt.Println("Merging databases and writing output...")
	}

	// Create merger and run
	m := merger.NewMerger(readers, cfg, rowWriter)
	if err := m.Merge(); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Flush writer
	if flusher, ok := rowWriter.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("failed to flush output: %w", err)
		}
	}

	if !quiet {
		elapsed := time.Since(startTime)
		fmt.Println()
		fmt.Printf("✓ Successfully completed in %v\n", elapsed.Round(time.Millisecond))
		fmt.Printf("Output written to: %s\n", cfg.Output.File)
	}

	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `mmdbconvert - Merge MaxMind MMDB databases and export to CSV or Parquet

USAGE:
    mmdbconvert [OPTIONS] <config-file>
    mmdbconvert --config <config-file> [OPTIONS]

OPTIONS:
    --config <file>    Path to TOML configuration file
    --quiet            Suppress progress output
    --help             Show this help message
    --version          Show version information

EXAMPLES:
    # Basic usage with config file
    mmdbconvert config.toml

    # Using explicit flag
    mmdbconvert --config config.toml

    # Suppress progress output
    mmdbconvert --config config.toml --quiet

CONFIGURATION:
    See docs/config.md for configuration file format and options.

MORE INFORMATION:
    Documentation: https://github.com/maxmind/mmdbconvert
    Report issues: https://github.com/maxmind/mmdbconvert/issues

`)
}
