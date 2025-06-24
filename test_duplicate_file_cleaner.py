import unittest
import tempfile
import shutil
from pathlib import Path
import io
from contextlib import redirect_stdout
from unittest.mock import patch
import openpyxl
import shutil # For test cleanup

from duplicate_file_cleaner import process_report_actions # Renamed function

class TestExecuteDeletionsFromReport(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.report_file = self.test_dir / "report.xlsx"

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def create_dummy_file(self, filename, content=b""):
        filepath = self.test_dir / filename
        with open(filepath, "wb") as f:
            f.write(content)
        return filepath

    def create_report_file(self, data, total_files_to_keep=None, total_size_to_keep_gb=None):
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "Files to Process"
        sheet.append(["Keep", "Size (Bytes)", "File 1", "File 2"]) 

        for row in data:
            sheet.append(row)

        # Add a dummy Summary sheet for copy tests to read from
        summary_sheet = workbook.create_sheet("Summary")
        summary_sheet["B2"] = total_files_to_keep if total_files_to_keep is not None else len(data) # Total Files to Keep
        summary_sheet["B4"] = total_size_to_keep_gb if total_size_to_keep_gb is not None else 0.00 # Total Size to Keep (GB)

        workbook.save(self.report_file)

    def test_execute_deletions_dry_run(self):
        file1 = self.create_dummy_file("file1.txt")
        file2 = self.create_dummy_file("file2.txt")
        data = [
            [1, 1024, "file1.txt", "file2.txt"]  # Keep file1.txt
        ]
        self.create_report_file(data)
        with redirect_stdout(io.StringIO()):
            process_report_actions(self.report_file, self.test_dir, dry_run=True, use_trash=False, copy_destination=None)

        self.assertTrue(file1.exists())
        self.assertTrue(file2.exists())  # Should not be deleted in dry run

    def test_execute_deletions_actual_deletion(self):
        file1 = self.create_dummy_file("file1.txt")
        file2 = self.create_dummy_file("file2.txt")
        data = [
            [1, 1024, "file1.txt", "file2.txt"]  # Keep file1.txt
        ]
        self.create_report_file(data)
        with redirect_stdout(io.StringIO()):
            process_report_actions(self.report_file, self.test_dir, dry_run=False, use_trash=False, copy_destination=None)

        self.assertTrue(file1.exists())
        self.assertFalse(file2.exists())  # Should be deleted

    def test_execute_deletions_invalid_keep_index(self):
        file1 = self.create_dummy_file("file1.txt")
        file2 = self.create_dummy_file("file2.txt")
        data = [
            [3, 1024, "file1.txt", "file2.txt"]  # Invalid index
        ]
        self.create_report_file(data)
        with redirect_stdout(io.StringIO()):
            process_report_actions(self.report_file, self.test_dir, dry_run=False, use_trash=False, copy_destination=None)

        self.assertTrue(file1.exists())
        self.assertTrue(file2.exists())  # Neither should be deleted

    def test_execute_deletions_missing_file(self):
        file1 = self.create_dummy_file("file1.txt")
        data = [
            [1, 1024, "file1.txt", "missing.txt"] # Missing file
        ]
        self.create_report_file(data)
        with redirect_stdout(io.StringIO()):
            process_report_actions(self.report_file, self.test_dir, dry_run=False, use_trash=False, copy_destination=None)
        self.assertTrue(file1.exists()) # Should not delete the existing file

    @patch('duplicate_file_cleaner.send2trash.send2trash')
    def test_execute_deletions_with_trash(self, mock_send2trash):
        file1 = self.create_dummy_file("file1.txt")
        file2 = self.create_dummy_file("file2.txt")
        data = [
            [1, 1024, "file1.txt", "file2.txt"]  # Keep file1.txt
        ]
        self.create_report_file(data) # No need for specific summary data for deletion
        with redirect_stdout(io.StringIO()):
            process_report_actions(self.report_file, self.test_dir, dry_run=False, use_trash=True, copy_destination=None)

        # Check that send2trash was called with the file to be deleted
        mock_send2trash.assert_called_once_with(file2)

    def test_process_report_actions_with_copy(self):
        # Create a file structure to test relative paths
        subdir = self.test_dir / "sub"
        subdir.mkdir()
        file1 = subdir / "file1.txt"
        file1.write_text("content1")
        file2 = subdir / "file2.txt"
        file2.write_text("content2")
        
        # Add a unique file to ensure it's also copied
        unique_file = self.test_dir / "unique.txt"
        unique_file.write_text("unique content")

        # Create a report where file1 is kept (from duplicate group) and unique.txt is also listed
        data = [
            [1, file1.stat().st_size, str(file1.relative_to(self.test_dir)), str(file2.relative_to(self.test_dir))], # Duplicate group
            [1, unique_file.stat().st_size, str(unique_file.relative_to(self.test_dir))] # Unique file
        ]
        # Calculate total size for summary sheet
        total_size_bytes = file1.stat().st_size + unique_file.stat().st_size
        total_size_gb = total_size_bytes / (1024**3)
        self.create_report_file(data, total_files_to_keep=2, total_size_to_keep_gb=f"{total_size_gb:.2f}")

        # Create a temporary destination for copying
        copy_dest_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, copy_dest_dir) # Clean up this temp dir too
        
        # Mock user input for confirmation
        with patch('builtins.input', return_value='y'), redirect_stdout(io.StringIO()):

            # Call the function in copy mode
            process_report_actions(self.report_file, self.test_dir, dry_run=False, use_trash=False, copy_destination=copy_dest_dir)

        # Assert that the kept file was copied
        copied_file1_path = copy_dest_dir / file1.relative_to(self.test_dir)
        self.assertTrue(copied_file1_path.exists())
        self.assertEqual(copied_file1_path.read_text(), "content1")

        # Assert that the unique file was copied
        copied_unique_file_path = copy_dest_dir / unique_file.relative_to(self.test_dir)
        self.assertTrue(copied_unique_file_path.exists())
        self.assertEqual(copied_unique_file_path.read_text(), "unique content")

        # Assert that the other file was NOT deleted (since it's copy mode)
        self.assertTrue(file2.exists())

        # Assert that the original kept file still exists
        self.assertTrue(file1.exists())

    def test_process_report_actions_copy_conflict_and_skip_identical(self):
        # Setup source files
        source_file_unique_content = self.create_dummy_file("unique_content.txt", b"unique_content")
        source_file_identical_content = self.create_dummy_file("identical_content.txt", b"identical_content")

        # Setup destination with conflicting files
        copy_dest_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, copy_dest_dir)
        
        # File at destination with different content
        dest_file_unique_content = copy_dest_dir / "unique_content.txt"
        dest_file_unique_content.write_text("different_content")

        # File at destination with identical content
        dest_file_identical_content = copy_dest_dir / "identical_content.txt"
        dest_file_identical_content.write_text("identical_content")

        # Create report to copy these files
        data = [
            [1, source_file_unique_content.stat().st_size, str(source_file_unique_content.relative_to(self.test_dir))],
            [1, source_file_identical_content.stat().st_size, str(source_file_identical_content.relative_to(self.test_dir))]
        ]
        # Calculate total size for summary sheet
        total_size_bytes = source_file_unique_content.stat().st_size + source_file_identical_content.stat().st_size
        total_size_gb = total_size_bytes / (1024**3)
        self.create_report_file(data, total_files_to_keep=2, total_size_to_keep_gb=f"{total_size_gb:.2f}")

        with patch('builtins.input', return_value='y'), redirect_stdout(io.StringIO()):
            process_report_actions(self.report_file, self.test_dir, dry_run=False, use_trash=False, copy_destination=copy_dest_dir)

        # Assert unique content file was copied with a new name
        self.assertTrue((copy_dest_dir / "unique_content_1.txt").exists())
        # Assert identical content file was skipped and original exists
        self.assertTrue(dest_file_identical_content.exists())
        self.assertEqual(dest_file_identical_content.read_text(), "identical_content")

    def test_process_report_actions_with_copy_dry_run(self):
        subdir = self.test_dir / "sub"
        subdir.mkdir()
        file1 = subdir / "file1.txt"
        file1.write_text("content1")
        file2 = subdir / "file2.txt"
        file2.write_text("content2")

        data = [
            [1, file1.stat().st_size, str(file1.relative_to(self.test_dir)), str(file2.relative_to(self.test_dir))],
            [1, 10, "unique_file.txt"] # Add a unique file for dry run
        ]
        # Create the unique file for the dry run test
        unique_file_path = self.create_dummy_file("unique_file.txt", b"unique content")
        
        # Calculate total size for summary sheet
        total_size_bytes = file1.stat().st_size + unique_file_path.stat().st_size
        total_size_gb = total_size_bytes / (1024**3)
        self.create_report_file(data, total_files_to_keep=2, total_size_to_keep_gb=f"{total_size_gb:.2f}")

        copy_dest_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, copy_dest_dir)

        with redirect_stdout(io.StringIO()): # Suppress print output
            # Call the function in copy mode with dry_run
            process_report_actions(self.report_file, self.test_dir, dry_run=True, use_trash=False, copy_destination=copy_dest_dir)

        # Assert that no files were actually copied
        self.assertFalse((copy_dest_dir / file1.relative_to(self.test_dir)).exists())
        self.assertTrue(file1.exists())
        self.assertTrue(file2.exists())
        self.assertFalse((copy_dest_dir / unique_file_path.relative_to(self.test_dir)).exists())
        self.assertTrue(unique_file_path.exists())
