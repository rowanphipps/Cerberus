"""Contains tests for the cerberus tool.

All tests should be run in Python 2 using python2 -m pytest
"""
import cerberus


def test_list(capsys, test_case):
    """Run a test on the cerberus list command."""
    cerberus.list_data(None, test_case["data"])
    out, _ = capsys.readouterr()
    assert out.rstrip() == "\n".join(test_case["list_test"])


def test_add_file_single(test_case):
    """Test adding a single file to a project."""
    data = test_case["data"]
    parser = cerberus.create_parser()
    args = parser.parse_args(["add-file", "autorun.exe"])
    output = cerberus.add_file(args, data)
    assert "autorun.exe" in output["files"]


def test_add_file_multiple(test_case):
    """Test adding multiple files to a project."""
    data = test_case["data"]
    parser = cerberus.create_parser()
    args = parser.parse_args(["add-file", "autorun.exe", "notavirus.bat"])
    output = cerberus.add_file(args, data)
    assert ("autorun.exe" in output["files"]
            and "notavirus.bat" in output["files"])


def test_remove_file(capsys, test_case):
    """Test removing a file from the project."""
    data = test_case["data"]
    parser = cerberus.create_parser()
    files = test_case["remove-file_test"]["files"]
    expected_output = "\n".join(test_case["remove-file_test"]["results"])
    expected_files = test_case["remove-file_test"]["final_files"]
    args = parser.parse_args(["remove-file"] + files)
    processed_data = cerberus.remove_files(args, data)
    messeges, _ = capsys.readouterr()
    assert messeges.strip() == expected_output
    assert set(processed_data["files"]) == set(expected_files)
