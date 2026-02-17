# for running get samples and providing args
def pytest_addoption(parser):
    parser.addoption("--username", action="store", default="FAKEUSER")
    parser.addoption("--password", action="store", default="FAKEPW")
    parser.addoption("--token", action="store", default="FAKETOKEN")

    
