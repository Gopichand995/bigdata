import pytest


@pytest.fixture()
def setup():
    print("\nThis executes before every method")


def test_method1(setup):
    print("This is test method1")


def test_method2(setup):
    print("This is test method2")
