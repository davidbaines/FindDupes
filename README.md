# FindDupes

A fast and reliable file duplicate finder with interactive cleanup and intelligent caching.

This command-line tool is designed to efficiently scan large collections of files (like photos and videos) to find duplicates, identify redundant folders, and provide safe, interactive tools for cleanup.

## Features

- **High-Speed Scanning:** Uses a multi-stage process (size, partial hash, full hash) to minimize disk I/O and find duplicates quickly.
- **Intelligent Caching:** Automatically caches scan results. Subsequent runs in the same directory are nearly instant if files haven't changed.
- **Advanced Folder Analysis:** Identifies not only exact duplicate folders but also folders that are complete subsets of others.
- **Interactive File Deletion:** A `--delete-files` mode guides you through deleting duplicate files, with an option to pivot to deleting entire folders.
- **Interactive Folder Deletion:** A separate `--interactive-delete` mode focuses on cleaning up redundant folders identified by the analysis.
- **Detailed Reporting:** Generates a comprehensive `.xlsx` report with separate tabs for file duplicates and redundant folders.
- **Standalone Executable:** Can be easily bundled into a single `.exe` file with PyInstaller for use on Windows machines without Python installed.

## How It Works

The script's speed comes from its multi-stage filtering approach and its caching mechanism.

1.  **Caching:** On subsequent runs, the script first checks for a `finddupes_cache.json` file. If a valid cache is found, the time-consuming scanning and hashing steps are skipped, and the program proceeds directly to the action phase.
2.  **Scanning (if no valid cache):**
    - **Stage 1 (Size):** Groups files by size. Files with a unique size cannot be duplicates and are ignored.
    - **Stage 2 (Partial Hash):** For same-sized files, it hashes the first and last 4KB. This quickly filters out most non-duplicates.
    - **Stage 3 (Full Hash):** Only for files that match in size and partial hash does it perform a full content hash to confirm duplication.

## Installation

This project uses Poetry for dependency management.

```shell
# Clone the repository
git clone https://github.com/davidbaines/FindDupes.git
cd FindDupes

# Install dependencies
poetry install
```

## Usage

The script is run from the command line, pointing it to the directory you want to scan.

### Generate a Report

This is the default behavior. It will scan the directory and create `duplicates_report.xlsx` and `finddupes_cache.json`.

```shell
poetry run python duplicate_finder.py "C:\Path\To\Your\Photos"
```

### Interactive File Deletion

Enter an interactive session to delete individual duplicate files.

```shell
poetry run python duplicate_finder.py "C:\Path\To\Your\Photos" --delete-files
```

### Interactive Folder Deletion

Enter an interactive session to delete redundant folders.

```shell
poetry run python duplicate_finder.py "C:\Path\To\Your\Photos" --interactive-delete
```

### Force a Rescan

To ignore the cache and force a complete new scan, use the `--force-rescan` flag.

```shell
poetry run python duplicate_finder.py "C:\Path\To\Your\Photos" --force-rescan
```

## Building the Executable

You can create a standalone `FindDupes.exe` for Windows using PyInstaller.

```shell
pyinstaller --onefile --name FindDupes duplicate_finder.py
```
The executable will be located in the `dist` folder.