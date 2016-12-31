"""Configuration for pytest."""
import json


def pytest_generate_tests(metafunc):
    """Configure pytest to call each of the tests once for each test case."""
    if "test_case" in metafunc.fixturenames:
        tests = json.load(open("tests/test_data.json"))["tests"]
        metafunc.parametrize("test_case", tests)
