import pytest


@pytest.yield_fixture()
def setup():
    print("\nThis is once before the test method")
    yield
    print("This is once after the test method")


def test_method1(setup):
    print("This is test method1")


def test_method2(setup):
    print("This is test method2")
