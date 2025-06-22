import os
from collections import defaultdict
from pathlib import Path

import pytest

# Assuming your script is named duplicate_finder.py
from duplicate_finder import (CHUNK_SIZE, analyze_folder_duplicates,
                              create_xlsx_report, find_duplicates,
                              process_interactive_file_deletions)


@pytest.fixture
def test_directory(tmp_path: Path) -> Path:
    """
    Creates a temporary directory with a structure for testing duplicate finding.
    - file1.txt and subdir/file1_copy.txt are duplicates.
    - large_file.bin and subdir/large_file_copy.bin are large duplicates.
    - partial_match.txt has the same first chunk as file1 but is different.
    - all other files are unique.
    """
    # Content for the files
    content1 = "This is the content for the first test file."
    content_partial_match = content1 + " But this file is longer."
    content_unique = "This file has completely unique content."
    content_large = os.urandom(CHUNK_SIZE * 2)  # Larger than one chunk

    # Create files
    (tmp_path / "file1.txt").write_text(content1)
    (tmp_path / "partial_match.txt").write_text(content_partial_match)
    (tmp_path / "unique.txt").write_text(content_unique)
    (tmp_path / "large_file.bin").write_bytes(content_large)
    (tmp_path / "empty.txt").touch()

    # Create a subdirectory and place duplicates there
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "file1_copy.txt").write_text(content1)
    (subdir / "large_file_copy.bin").write_bytes(content_large)

    return tmp_path


@pytest.fixture
def folder_test_directory(tmp_path: Path) -> Path:
    """
    Creates a directory structure for testing folder relationships.
    - dir_A is an EXACT DUPLICATE of dir_C.
    - dir_A is a SUBSET of dir_B.
    """
    content_a = "Content A"
    content_b = "Content B"
    content_c = "Unique content for dir_B"

    dir_a = tmp_path / "dir_A"
    dir_a.mkdir()
    (dir_a / "file1.txt").write_text(content_a)
    (dir_a / "file2.txt").write_text(content_b)

    dir_b = tmp_path / "dir_B"
    dir_b.mkdir()
    (dir_b / "file1_copy.txt").write_text(content_a)
    (dir_b / "file2_copy.txt").write_text(content_b)
    (dir_b / "unique_in_B.txt").write_text(content_c)

    dir_c = tmp_path / "dir_C"
    dir_c.mkdir()
    (dir_c / "file1_another_copy.txt").write_text(content_a)
    (dir_c / "file2_another_copy.txt").write_text(content_b)

    return tmp_path


def test_find_duplicates_successfully(test_directory: Path):
    """
    Tests that the script correctly identifies all duplicate sets.
    """
    duplicate_sets, _, _ = find_duplicates(test_directory)

    # We expect to find 2 sets of duplicates
    assert len(duplicate_sets) == 2, "Should find exactly two sets of duplicates"

    # To make assertions reliable, sort the paths within each result set
    # and then sort the sets themselves based on the first file path.
    sorted_results = sorted(
        [sorted(group) for group in duplicate_sets], key=lambda x: x[0]
    )

    # Define expected results
    expected_set1 = sorted(
        [test_directory / "file1.txt", test_directory / "subdir" / "file1_copy.txt"]
    )
    expected_set2 = sorted(
        [
            test_directory / "large_file.bin",
            test_directory / "subdir" / "large_file_copy.bin",
        ]
    )

    # Assert that the found duplicates match the expected ones
    assert sorted_results[0] == expected_set1
    assert sorted_results[1] == expected_set2


def test_no_duplicates_found(tmp_path: Path):
    """
    Tests that the script returns an empty list when no duplicates exist.
    """
    (tmp_path / "file1.txt").write_text("hello")
    (tmp_path / "file2.txt").write_text("world")

    duplicate_sets, _, _ = find_duplicates(tmp_path)
    assert duplicate_sets == [], "Should not find any duplicates"


def test_empty_directory(tmp_path: Path):
    """
    Tests that the script handles an empty directory gracefully.
    """
    duplicate_sets, _, _ = find_duplicates(tmp_path)
    assert duplicate_sets == [], "Should return an empty list for an empty directory"


def test_nonexistent_directory():
    """
    Tests that the script raises a ValueError for a non-existent directory.
    """
    with pytest.raises(
        ValueError, match="'non_existent_dir' is not a valid directory."
    ):
        find_duplicates(Path("non_existent_dir"))


def test_create_xlsx_report(test_directory: Path, capsys):
    """
    Tests that the XLSX report is created correctly with the expected data.
    """
    try:
        import openpyxl
    except ImportError:
        pytest.skip("openpyxl not installed, skipping report test")

    # 1. Find duplicates first. Use capsys to keep test output clean.
    duplicate_sets, all_files, full_hashes_map = find_duplicates(test_directory)
    capsys.readouterr()  # Clear captured stdout/stderr

    assert len(duplicate_sets) == 2, "Test setup should yield two duplicate sets."

    # 2. Create the report
    folder_analysis = analyze_folder_duplicates(all_files, full_hashes_map)
    create_xlsx_report(duplicate_sets, folder_analysis, test_directory)

    # 3. Verify the report file exists
    report_path = test_directory / "duplicates_report.xlsx"
    assert report_path.exists(), "XLSX report file was not created."

    # 4. Load and verify the content of the report
    workbook = openpyxl.load_workbook(report_path)
    sheet = workbook.active
    rows = list(sheet.values)

    # Check header
    expected_header = (
        "Set ID",
        "File Path",
        "Size (Bytes)",
        "File Type",
        "Date Created",
        "Date Modified",
    )
    assert rows[0] == expected_header, "XLSX header is incorrect."

    # Check number of data rows (2 sets of 2 files = 4 data rows)
    assert len(rows) == 5, "XLSX should have 1 header row and 4 data rows."

    # Check that the correct files were reported by comparing sets of paths
    reported_paths = {row[1] for row in rows[1:]}
    expected_paths = {
        str(test_directory / "file1.txt"),
        str(test_directory / "subdir" / "file1_copy.txt"),
        str(test_directory / "large_file.bin"),
        str(test_directory / "subdir" / "large_file_copy.bin"),
    }
    assert (
        reported_paths == expected_paths
    ), "The file paths in the report are incorrect."


def test_analyze_folder_duplicates_subset_and_exact(folder_test_directory: Path):
    """
    Tests that the folder analysis correctly identifies subset and exact duplicate folders.
    """
    _, all_files, full_hashes_map = find_duplicates(folder_test_directory)
    relationships, _ = analyze_folder_duplicates(all_files, full_hashes_map)

    assert (
        len(relationships) == 3
    ), "Should find two subset and one exact duplicate relationship."

    # Unpack relationships for easier assertion
    rel_counts = defaultdict(int)
    for rel in relationships:
        rel_counts[rel["type"]] += 1

    assert rel_counts["Subset"] == 2
    assert rel_counts["Exact Duplicate"] == 1

    for rel in relationships:
        if rel["type"] == "Subset":
            # Check that both dir_A and dir_C are subsets of dir_B
            assert rel["superset"] == folder_test_directory / "dir_B"
            assert rel["subset"] in {
                folder_test_directory / "dir_A",
                folder_test_directory / "dir_C",
            }
        elif rel["type"] == "Exact Duplicate":
            # The order of folder_a and folder_b isn't guaranteed, so check both
            assert {rel["folder_a"], rel["folder_b"]} == {
                folder_test_directory / "dir_A",
                folder_test_directory / "dir_C",
            }


def test_interactive_file_deletion(test_directory: Path, monkeypatch):
    """
    Tests the interactive file deletion by simulating user input.
    """
    # Find duplicates first
    duplicate_sets, _, _ = find_duplicates(test_directory)
    file_to_keep = test_directory / "file1.txt"
    file_to_delete = test_directory / "subdir" / "file1_copy.txt"

    # Find the specific group we want to test with
    target_group = None
    for group in duplicate_sets:
        if file_to_keep in group:
            target_group = group
            break
    assert target_group is not None, "Test setup failed: could not find target group."

    # To make the test robust, determine which input to provide based on sorted order
    sorted_group = sorted(target_group)
    index_to_keep = sorted_group.index(file_to_keep)
    user_input = str(index_to_keep + 1)
    monkeypatch.setattr("builtins.input", lambda _: user_input)

    process_interactive_file_deletions([target_group])

    assert file_to_keep.exists(), "The file chosen to be kept was deleted."
    assert not file_to_delete.exists(), "The file chosen for deletion still exists."
