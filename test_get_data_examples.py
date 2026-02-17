import pytest
import json
import gql

from monarch.monarch import *

from monarchmoney import MonarchMoney


@pytest.fixture
def username(request):
    return request.config.getoption("username")


@pytest.fixture
def password(request):
    return request.config.getoption("password")


@pytest.fixture
def token(request):
    return request.config.getoption("token")


@pytest.mark.skip(reason="don't hit the server unless explicitly trying to do so.")
@pytest.mark.asyncio
async def test_wrapper_login(username, password, token):
    m = Monarch(un=username, pw=password, token=token)
    # note we didn't invoke login, first attempt will fail and retry logic will be invoked.
    txs = await m.get_transactions()
    assert len(txs) > 0


@pytest.mark.skip(reason="don't hit the server unless explicitly trying to do so.")
@pytest.mark.asyncio
async def test_get_categories(username, password, token):
    assert username == "kyle.d.duncan@gmail.com"

    mm = MonarchMoney()
    try:
        mm.load_session()
    except FileNotFoundError as e:
        await mm.login(
            email=username,
            password=password,
            save_session=False,
            use_saved_session=False,
            mfa_secret_key=token,
        )
        mm.save_session()

    txs = await mm.get_transaction_categories()
    with open("data_examples/categories.json", "w") as f:
        json.dump(txs, f, indent=4)

    return

@pytest.mark.skip(reason="don't hit the server unless explicitly trying to do so.")
@pytest.mark.asyncio
async def test_get_transactions(username, password, token):
    assert username == "kyle.d.duncan@gmail.com"

    mm = MonarchMoney()
    try:
        mm.load_session()
    except FileNotFoundError as e:
        await mm.login(
            email=username,
            password=password,
            save_session=False,
            use_saved_session=False,
            mfa_secret_key=token,
        )
        mm.save_session()

    txs = await mm.get_transactions(
        limit=3000, start_date="2026-01-01", end_date="2026-01-13"
    )
    with open("data_examples/transactions.json", "w") as f:
        json.dump(txs, f, indent=4)

    return

    
@pytest.mark.skip(reason="don't hit the server unless explicitly trying to do so.")
@pytest.mark.asyncio
async def test_get_samples(username, password, token):

    assert username == "kyle.d.duncan@gmail.com"

    mm = MonarchMoney()
    try:
        mm.load_session()
    except FileNotFoundError as e:
        await mm.login(
            email=username,
            password=password,
            save_session=False,
            use_saved_session=False,
            mfa_secret_key=token,
        )
        mm.save_session()

    txs = await mm.get_transactions(
        limit=3000, start_date="2026-01-01", end_date="2026-01-13"
    )
    with open("transactions.json", "w") as f:
        json.dump(txs, f, indent=4)

    result = await mm.get_accounts()

    with open("accounts.json", "w") as f:
        json.dump(result, f, indent=4)

    snapshots = await mm.get_aggregate_snapshots(
        start_date="2026-01-01", end_date="2026-01-13"
    )

    with open("snapshots.json", "w") as f:
        json.dump(snapshots, f, indent=4)

    recent_balances = await mm.get_recent_account_balances(start_date="2026-01-12")
    with open("recent_balances.json", "w") as f:
        json.dump(recent_balances, f, indent=4)

    holdings = await mm.get_account_holdings("232706677260014485")
    with open("vanguard_taxable_holdings.json", "w") as f:
        json.dump(holdings, f, indent=4)

    holdings = await mm.get_account_holdings("232531953123971666")
    with open("transamerica_403b_holdings.json", "w") as f:
        json.dump(holdings, f, indent=4)

    return
