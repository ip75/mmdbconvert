// mmdbconvert merges multiple MaxMind MMDB databases and exports to CSV or Parquet format.
package main

import (
	"flag"
	"fmt"
	"os"
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

	// TODO: Implement main processing logic
	fmt.Printf("mmdbconvert v%s\n", version)
	fmt.Printf("Config file: %s\n", configPath)
	fmt.Printf("Quiet mode: %v\n", quiet)
	fmt.Println("\nProcessing not yet implemented.")
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
