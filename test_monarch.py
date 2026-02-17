import pytest
import json
import gql
import monarchmoney
import pytest_httpserver
import pathlib
import shutil
import itertools
import marshmallow
import marshmallow.experimental.context

from monarch.monarch import *


@pytest.fixture(scope="session", autouse=True)
def test_out():
    # wipe old output if exists
    # create out dir
    out = pathlib.Path("out")
    out.mkdir(parents=False, exist_ok=True)

    for item in itertools.chain(out.glob("*.csv"), out.glob("*.pickle")):
        if item.is_file():
            item.unlink()

    # let tests see the path. more convenient to not be a Path object so we can just append subpaths
    yield "out"
    # could wipe here but want the contents visible for inspection.


@pytest.fixture()
def m(request, test_out):
    pickle_file = test_out + "/" + request.node.name + "_session.pickle"
    existing_session = request.param
    # want a literal boolean true; not just anything truthey
    if existing_session == True:
        # pre-create the session file with syntactically valid content.
        shutil.copy("data_examples/mm_session.pickle", pickle_file)
        pass

    m = Monarch(
        un="blah",
        pw="blah",
        token="blah",
        session_file=pickle_file,
        rb=test_out + "/" + request.node.name + "rb.csv",
        rbh=test_out + "/" + request.node.name + "rbh.csv",
        rt=test_out + "/" + request.node.name + "rt.csv",
        rp=test_out + "/" + request.node.name + "rp.csv",
        retry_delay=0,
    )
    yield m
    # no particular teardown


# no constants available from underlying library. in fairness we're
# exercising their internals rather than mocking them, an invasive and
# potentially fragile approach but that helps ensure my own code
# interacts with theirs as I expect.
MM_GRAPHQL_URL = "/graphql"
MM_AUTH_URL = "/auth/login/"


# don't need to read the file more than once but wonder if it's wise
# to expose an object that might get modifeed and rely on it across
# two functions
@pytest.fixture(scope="session")
def transaction_data():
    with open("data_examples/transactions.json", "r") as f:
        data = json.load(f)
    yield data


@pytest.fixture(scope="session")
def category_data():
    with open("data_examples/categories.json", "r") as f:
        data = json.load(f)
    yield data


@pytest.fixture(scope="session")
def accounts_data():
    with open("data_examples/accounts.json", "r") as f:
        data = json.load(f)
    yield data


@pytest.fixture(scope="session")
def holdings_data():
    with open("data_examples/holdings.json", "r") as f:
        data = json.load(f)
    yield data


@pytest.fixture(scope="function")
def local_url(mocker, httpserver: pytest_httpserver.HTTPServer):
    # pytest_httpserver defaults to 127.0.0.1:<dynamic port>
    # use url_for to get URL including the dynamic port
    mocker.patch.object(
        monarchmoney.MonarchMoneyEndpoints, "BASE_URL", httpserver.url_for("/")
    )

    # nothing to yield as we do not access the mocked object directly within the test.
    pass


def test_CategoryQuerySchema_load(category_data):
    # read categories from disk
    # load it, check the map output
    cqs = CategoryQuerySchema()
    catmap = cqs.load(category_data)
    assert catmap["232525884431651171"] == "Home"
    assert len(catmap) == 80


def test_convert_transactions(transaction_data, category_data):

    cqs = CategoryQuerySchema()
    catmap = cqs.load(category_data)
    schema = TransactionsQuerySchema()

    with TransactionCategorySchemaContext({"catmap": catmap}):
        loaded_data = schema.load(transaction_data)

    # assert how many records
    assert len(loaded_data) == 89
    # assert one of the records has right stuff.
    assert loaded_data[0].account == "exampleAccount"
    assert loaded_data[0].group == "Personal"

    return


def test_unmarshall_single_holdingsquery(holdings_data):
    schema = HoldingsQuerySchema()
    loaded_data = schema.load(holdings_data)

    assert len(loaded_data) == 2
    # account field will be null. is written by a parent before being fed to csv.
    assert loaded_data[0].ticker == "AAA"
    assert loaded_data[0].shares == "1288.212"
    assert loaded_data[0].price == "33.03"
    assert loaded_data[0].cost == "2227.6"
    assert loaded_data[1].ticker == "BBB"

    return


@pytest.mark.asyncio
# tell m fixture to pre-create a session file
@pytest.mark.parametrize("m", [True], indirect=True)
async def test_get_transactions_401_single_retry(
    httpserver: pytest_httpserver.HTTPServer,
    local_url,
    mocker,
    transaction_data,
    request,
    test_out,
    m,
):

    # instantiate our monarch api, give it a stale session file.
    # it will read the session we know is stale, and not attempt to login directly yet.
    # ask it to get transactions; it will hit our mocked server. respond with a 401 as the real server would
    # verify our api responds by re-logging-in, then retrying the get-transactions
    # on the second attempt to get transactions, return data and conclude.

    assert httpserver.is_running()

    # warning - these URLs are sensitive to a trailing slash.
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_data(
        "KDD ERROR", status=401, content_type="text/plain"
    )
    httpserver.expect_ordered_request(MM_AUTH_URL).respond_with_json(
        {"token": "FAKETOKEN"}, status=200
    )
    # finally, return some actual transaction data using our data samples

    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": transaction_data}, status=200
    )

    login_spy = mocker.spy(m, "login")

    # load the saved session
    # but want not to invoke this until after we've had a chance to spy
    await m.login()

    # should raise 401 right now, until we get our retry logic
    # now we need to try to add some tenacity behavior and assert they were called.
    txs = await m.get_transactions()

    # method will retry; this should fail as being called twice
    assert login_spy.call_count == 2
    print(login_spy.call_args_list)

    # assert something about the data returned
    assert txs["allTransactions"]["totalCount"] == 89


@pytest.mark.asyncio
# tell m fixture to pre-create a session file
@pytest.mark.parametrize("m", [True], indirect=True)
async def test_report_transactions_success(
    httpserver: pytest_httpserver.HTTPServer,
    local_url,
    mocker,
    category_data,
    transaction_data,
    request,
    m,
):

    assert httpserver.is_running()

    # pretend for this test that the use of a pickled session worked and we don't need to fire a login call to the server.
    # return some actual transaction data using our data samples
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": category_data}, status=200
    )
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": transaction_data}, status=200
    )

    await m.report_transactions()

    # check the existence of the file
    with open(m.fn_tx_report, "r") as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        # spot check just a couple things for now.
        assert header == TRANSACTIONS_HEADER
        first_row = next(csv_reader)
        assert "Clothing" in first_row
        pass


def test_unmarshall_accounts(accounts_data):
    schema = AccountsQuerySchema()
    # still a schema loaded dict not an object. consider fixing naming convention to *Schema so I can post-load another object
    loaded_data = schema.load(accounts_data)
    assert len(loaded_data) == 23
    assert loaded_data[0].account == "Checking"
    assert str(loaded_data[0].balance) == "1811.71"
    assert str(loaded_data[0].date_eastern) == "2026-01-12"
    assert str(loaded_data[0].datetime) == "2026-01-12T14:28:13.637497+00:00"
    return


@pytest.mark.asyncio
# tell m fixture to pre-create a session file
@pytest.mark.parametrize("m", [True], indirect=True)
async def test_report_balances_success(
    httpserver: pytest_httpserver.HTTPServer,
    local_url,
    mocker,
    accounts_data,
    request,
    m,
):

    assert httpserver.is_running()

    # pretend for this test that the use of a pickled session worked and we don't need to fire a login call to the server.
    # return some actual transaction data using our data samples
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": accounts_data}, status=200
    )
    # we're running twice to test appending of balance history
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": accounts_data}, status=200
    )

    account_data = await m.report_balances()
    checking_row = [
        "Checking",
        "1811.71",
        "2026-01-12",
        "2026-01-12T14:28:13.637497+00:00",
    ]

    assert len(account_data) == 23

    # check the existence of the file
    with open(m.fn_balance_report, "r") as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        # spot check just a couple things for now.
        assert header == BALANCES_HEADER

        first_row = next(csv_reader)
        print(first_row)
        assert first_row == checking_row

        pass

    # check the existence of the history file
    with open(m.fn_balance_history_report, "r") as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)
        # one copy of the file
        assert len(data) == 24
        assert data[0] == BALANCES_HEADER
        assert data[1] == checking_row

    # write again
    await m.report_balances()
    # prove balances overwritten and history appended but without dup header.
    with open(m.fn_balance_report, "r") as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)
        assert len(data) == 24

    # check the existence of the history file
    with open(m.fn_balance_history_report, "r") as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)
        # two sets of account data plus one header
        assert len(data) == 47
        assert data[0] == BALANCES_HEADER
        assert data[1] == checking_row
        # would be header again except we detected non-empty file and went ahead to next data row.
        assert data[24] == checking_row


@pytest.mark.asyncio
# tell m fixture to pre-create a session file
@pytest.mark.parametrize("m", [True], indirect=True)
async def test_get_accounts_401(
    httpserver: pytest_httpserver.HTTPServer, local_url, mocker, accounts_data, m
):

    # instantiate our monarch api, give it a stale session file.
    # it will read the session we know is stale, and not attempt to login directly yet.
    # ask it to get accounts; it will hit our mocked server. respond with a 401 as the real server would
    # verify our api responds by re-logging-in, then retrying the get-transactions
    # on the second attempt to get accounts, return data and conclude.

    assert httpserver.is_running()

    # warning - these URLs are sensitive to a trailing slash.
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_data(
        "KDD ERROR", status=401, content_type="text/plain"
    )
    httpserver.expect_ordered_request(MM_AUTH_URL).respond_with_json(
        {"token": "FAKETOKEN"}, status=200
    )

    # finally, return some actual transaction data using our data samples

    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": accounts_data}, status=200
    )

    login_spy = mocker.spy(m, "login")

    # load the saved session. won't make any http calls.
    await m.login()

    # try to get data; receive 401. retry getting data, receive 200 and data.
    accounts = await m.get_accounts()
    acs = AccountsQuerySchema()
    accounts_query = acs.load(accounts)

    assert login_spy.call_count == 2
    assert len(accounts_query) == 23
    return


@pytest.mark.asyncio
# tell m fixture to pre-create a session file
@pytest.mark.parametrize("m", [True], indirect=True)
async def test_get_holdings_401(
    httpserver: pytest_httpserver.HTTPServer,
    local_url,
    mocker,
    holdings_data,
    m,
):
    assert httpserver.is_running()

    # warning - these URLs are sensitive to a trailing slash.
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_data(
        "KDD ERROR", status=401, content_type="text/plain"
    )
    httpserver.expect_ordered_request(MM_AUTH_URL).respond_with_json(
        {"token": "FAKETOKEN"}, status=200
    )

    # finally, return some actual transaction data using our data samples

    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": holdings_data}, status=200
    )

    login_spy = mocker.spy(m, "login")
    mm_get_account_holdings_spy = mocker.spy(m.mm, "get_account_holdings")

    # load the saved session. won't make any http calls.
    await m.login()

    # try to get data; receive 401. retry getting data, receive 200 and data.
    test_id = "test_id"
    holdings = await m.get_holdings(test_id)
    hqs = HoldingsQuerySchema()
    holdings_query = hqs.load(holdings)

    assert login_spy.call_count == 2
    assert len(holdings_query) == 2
    # account name not available from within the above query.
    assert holdings_query[0].ticker == "AAA"
    assert holdings_query[0].shares == "1288.212"
    assert holdings_query[0].price == "33.03"
    assert holdings_query[0].cost == "2227.6"
    mm_get_account_holdings_spy.assert_called_with(test_id)
    mm_get_account_holdings_spy.call_count == 1
    return


@pytest.mark.asyncio
# tell m fixture to pre-create a session file
@pytest.mark.parametrize("m", [True], indirect=True)
async def test_report_portfolio_success(
    httpserver: pytest_httpserver.HTTPServer,
    local_url,
    mocker,
    accounts_data,
    holdings_data,
    request,
    m,
):

    assert httpserver.is_running()

    # 6 calls try to get holdings of various accounts; collect the IDs we query and check them.
    # for each return some canned info.
    for i in range(1, 7):
        httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
            {"data": holdings_data}, status=200
        )

    schema = AccountsQuerySchema()
    await m.report_portfolio(schema.load(accounts_data))

    # check the existence of the file

    with open(m.fn_portfolio_report, "r") as f:
        csv_reader = csv.DictReader(f)

        # spot check just a couple things for now.
        assert csv_reader.fieldnames == PORTFOLIO_HEADER

        count = 0
        for row in csv_reader:
            count = count + 1
            # weak and sloppy contents check
            assert row["ticker"] == "AAA" or row["ticker"] == "BBB"

    return


def test_constructor():
    # ignoring session file arg for the moment as it's not my typical test case and I don't want the monarchmoney code parsing here,
    # and am too lazy to mock it until I have to.
    m = Monarch(un="1", pw="2", token="3", rt="4", rb="5", rp="6")

    assert m.un == "1"
    assert m.pw == "2"
    assert m.token == "3"
    assert m.fn_tx_report == "4"
    assert m.fn_balance_report == "5"
    assert m.fn_portfolio_report == "6"
    return


@pytest.mark.asyncio
async def test_main_blank_args(
    httpserver: pytest_httpserver.HTTPServer, local_url, mocker, request, test_out
):
    argument_list = [
        "monarch.py",
        # "--username","blah@gmail.com",
        "--password",
        "password",
        "--token",
        "token",
        "--session",
        "blah",
        "--report_balances",
        "blah",
        "--report_transactions",
        "blah",
        "--report_portfolio",
        "blah",
    ]
    try:
        # test the slice syntax we're using too. should omit the monarch.py
        # missing required fields should raise error from parse_args
        # fields provided but blank should raise from not_empty
        # thus far - none are doing so. and somehow we're getting blanks all the way down on any real run.
        await main(argument_list[1:])

        # we should be raising an error and are not. Let's start spying on the arguments on their way down.
        assert False
        # my runtime is getting past this somehow.
    except SystemExit:
        pass
    return


@pytest.mark.asyncio
async def test_main(
    httpserver: pytest_httpserver.HTTPServer,
    local_url,
    mocker,
    request,
    accounts_data,
    category_data,
    transaction_data,
    holdings_data,
    test_out,
):

    # white box test must know the order of calls within in order to mock the http responses in order. fragile.
    # could use a spy to wrap calls with data prep but that's not really any better.

    # login-on-401 behavior tested elsewhere

    # login call will look for a pickled session. this test provides one that shall not exist pre-test,
    # ensuring that a login will be called.
    httpserver.expect_ordered_request(MM_AUTH_URL).respond_with_json(
        {"token": "FAKETOKEN"}, status=200
    )

    # balances calls first
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": accounts_data}, status=200
    )
    # categories next
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": category_data}, status=200
    )

    # transactions next
    httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
        {"data": transaction_data}, status=200
    )

    # then holdings exactly 6 times. I can't find such a expectation in pytest_httpserver so just dup the expectation here.
    for i in range(1, 7):
        httpserver.expect_ordered_request(MM_GRAPHQL_URL).respond_with_json(
            {"data": holdings_data}, status=200
        )

    out_b = test_out + "/" + request.node.name + "_balances.csv"
    out_h = test_out + "/" + request.node.name + "_balances_history.csv"    
    out_t = test_out + "/" + request.node.name + "_transactions.csv"
    out_p = test_out + "/" + request.node.name + "_portfolio.csv"
    # simulate a command string
    argument_list = [
        "monarch.py",
        "--username",
        "blah@gmail.com",
        "--password",
        "password",
        "--token",
        "token",
        "--session",
        test_out + "/" + "test_main_session.pickle",
        "--report_balances",
        out_b,
        "--report_balances_history",
        out_h,        
        "--report_transactions",
        out_t,
        "--report_portfolio",
        out_p,
    ]

    await main(argument_list[1:])

    # check for the expected outputs. for now rely on the other tests having checked the contents.
    assert pathlib.Path(out_b).exists()
    assert pathlib.Path(out_t).exists()
    assert pathlib.Path(out_p).exists()
    return
