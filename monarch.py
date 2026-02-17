# builtins
import typing
import csv
import json
import datetime
import argparse
import sys
import asyncio

# 3rd-party
import marshmallow
import marshmallow.experimental.context
import monarchmoney
import tenacity
import gql
import zoneinfo

PORTFOLIO_HEADER = ["account", "ticker", "shares", "price", "cost"]
TRANSACTIONS_HEADER = [
    "date",
    "merchant",
    "category",
    "group",
    "account",
    "notes",
    "amount",
]
BALANCES_HEADER = ["account", "balance", "date_eastern", "datetime"]
RECORD_LIMIT_TX = 10000
FN_TX_REPORT = "transactions.csv"
FN_BALANCE_REPORT = "balance.csv"
FN_BALANCE_HISTORY_REPORT = "balance_history.csv"
FN_PORTFOLIO_REPORT = "portfolio.csv"
RETRY_DELAY = 2


# for tenacity
def is_exception_401(exception):
    return exception is not None and exception.code == 401


# for tenacity
async def login_before_sleep(retry_state: tenacity.RetryCallState):
    instance = retry_state.args[0]
    await instance.login(False)


# for tenacity. we want the wait time configurable at the Monarch instance,
# for unit test overrides if nothing else.
def wait_from_instance(retry_state: tenacity.RetryCallState):
    instance = retry_state.args[0]
    return instance.retry_delay


class GroupSchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    name = marshmallow.fields.Str()

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class CategoryContext(typing.TypedDict):
    catmap: typing.Dict[str, str]


class CategorySchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    name = marshmallow.fields.Str()
    group = marshmallow.fields.Nested(GroupSchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class CategoryQuerySchema(marshmallow.Schema):
    categories = marshmallow.fields.List(marshmallow.fields.Nested(CategorySchema))

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE

    @marshmallow.post_load
    def make_map(self, data, **kwargs):
        # make a map of category ids to group name
        catmap = {}
        for x in data["categories"]:
            catmap[str(x["id"])] = x["group"]["name"]

        return catmap


class AccountSchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    displayName = marshmallow.fields.Str()
    currentBalance = marshmallow.fields.Decimal(as_string=True)
    holdingsCount = marshmallow.fields.Int()
    updatedAt = marshmallow.fields.DateTime()

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class SecuritySchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    name = marshmallow.fields.Str()
    ticker = marshmallow.fields.Str()
    currentPrice = marshmallow.fields.Decimal(as_string=True)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class AggregateHoldingSchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    quantity = marshmallow.fields.Decimal(as_string=True)
    basis = marshmallow.fields.Decimal(as_string=True)
    totalValue = marshmallow.fields.Decimal(as_string=True)
    security = marshmallow.fields.Nested(SecuritySchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class AggregateHoldingEdgeSchema(marshmallow.Schema):
    node = marshmallow.fields.Nested(AggregateHoldingSchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class AggregateHoldingsConnectionSchema(marshmallow.Schema):
    edges = marshmallow.fields.List(
        marshmallow.fields.Nested(AggregateHoldingEdgeSchema)
    )

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class PortfolioSchema(marshmallow.Schema):
    aggregateHoldings = marshmallow.fields.Nested(AggregateHoldingsConnectionSchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class HoldingsQuerySchema(marshmallow.Schema):
    """
    Represent investment holdings at a single bank. Won't know its own name.
    """

    portfolio = marshmallow.fields.Nested(PortfolioSchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE

    @marshmallow.post_load
    def make_csv_rows(self, data, **kwargs):
        return [Holding(row) for row in data["portfolio"]["aggregateHoldings"]["edges"]]


class MerchantSchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    name = marshmallow.fields.Str()

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class CategorySchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    name = marshmallow.fields.Str()

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class TransactionSchema(marshmallow.Schema):
    id = marshmallow.fields.Int(required=True)
    amount = marshmallow.fields.Decimal(as_string=True)
    date = marshmallow.fields.Str()
    notes = marshmallow.fields.Str(allow_none=True)
    merchant = marshmallow.fields.Nested(MerchantSchema)
    account = marshmallow.fields.Nested(AccountSchema)
    category = marshmallow.fields.Nested(CategorySchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


class TransactionListSchema(marshmallow.Schema):
    results = marshmallow.fields.List(marshmallow.fields.Nested(TransactionSchema))

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE


TransactionCategorySchemaContext = marshmallow.experimental.context.Context[
    CategoryContext
]


class TransactionsQuerySchema(marshmallow.Schema):
    allTransactions = marshmallow.fields.Nested(TransactionListSchema)

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE

    @marshmallow.post_load
    def make_csv_rows(self, data, **kwargs):
        catmap = TransactionCategorySchemaContext.get()["catmap"]
        return [Transaction(row, catmap) for row in data["allTransactions"]["results"]]


class AccountsQuerySchema(marshmallow.Schema):
    accounts = marshmallow.fields.List(marshmallow.fields.Nested(AccountSchema))

    class Meta:
        # Ignore unknown fields
        unknown = marshmallow.EXCLUDE

    @marshmallow.post_load
    def make_csv_rows(self, data, **kwargs):
        return [Account(row) for row in data["accounts"]]


class Holding:
    def __init__(self, row):
        node = row["node"]
        self.account = (
            None  # held at higher level in the gql; don't read from input row.
        )
        self.ticker = node["security"]["ticker"]
        # unclear why the as_string option doesn't get applied at read; apparently only does upon marshalling.
        self.shares = str(node["quantity"])
        self.price = str(node["security"]["currentPrice"])
        self.cost = str(node["basis"])


class Transaction:
    """
    Used to flatten GQL-shaped Marshmallow into a flat object for CSV writing
    """

    def __init__(self, row, catmap):
        # row is a dict from a marshmallow transactionList.results
        self.date = row["date"]
        self.merchant = row["merchant"]["name"]
        self.category = row["category"]["name"]
        self.group = catmap[str(row["category"]["id"])]
        self.account = row["account"]["displayName"]
        self.notes = row["notes"]
        self.amount = row["amount"]


class Account:
    def __init__(self, row):
        self.id = row["id"]
        self.account = row["displayName"]
        self.balance = row["currentBalance"]
        self.holdingsCount = row["holdingsCount"]
        self.datetime = row["updatedAt"].isoformat()

        # convert to eastern timezone
        self.date_eastern = (
            row["updatedAt"]
            .astimezone(zoneinfo.ZoneInfo("America/New_York"))
            .date()
            .isoformat()
        )


# TODO type hints on arguments and returns
class Monarch(object):

    async def login(self, use_saved_session=True):
        # monarchmoney, after 401, still has _headers with a stale auth token,
        # and provides no way to clear it. on attempt to re-log-in, it gives that bad token as
        # part of the request and gets another 401.
        # to avoid this we need a fresh monarchmoney instance that is told not to use a saved session on login
        # we also need to set the session file path since we allow that to be overridden, meaning we have to hold onto it.
        if not use_saved_session:
            # force clear
            self._init_mm()

        await self.mm.login(
            email=self.un,
            password=self.pw,
            save_session=True,
            use_saved_session=use_saved_session,
            mfa_secret_key=self.token,
        )

    def _init_mm(self):
        if self.session_file:
            self.mm = monarchmoney.MonarchMoney(session_file=self.session_file)
        else:
            # don't override the built-in default value if we don't have an actual value ourselves.
            self.mm = monarchmoney.MonarchMoney()
        pass

    # allow override session file location for unit test purposes. use their defualt, though.
    def __init__(
        self,
        un,
        pw,
        token,
        session_file=None,
        rb=FN_BALANCE_REPORT,
        rbh=FN_BALANCE_HISTORY_REPORT,
        rt=FN_TX_REPORT,
        rp=FN_PORTFOLIO_REPORT,
        retry_delay=RETRY_DELAY,
    ):
        assert un is not None and len(un) > 0
        assert token is not None and len(token) > 0
        assert pw is not None and len(pw) > 0
        self.un = un
        self.pw = pw
        self.token = token
        self.fn_tx_report = rt
        self.fn_balance_report = rb
        self.fn_balance_history_report = rbh
        self.fn_portfolio_report = rp
        self.retry_delay = retry_delay
        self.session_file = session_file
        self._init_mm()

    def write_transactions(self, transactions):
        with open(self.fn_tx_report, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=TRANSACTIONS_HEADER, quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            for tx in transactions:
                writer.writerow(tx.__dict__)

    # on a failed login, we need to ignore old auth session and make a new one.
    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            gql.transport.exceptions.TransportServerError
        )
        & tenacity.retry_if_exception(is_exception_401),
        # force ignore old session on 401
        before_sleep=login_before_sleep,
        stop=tenacity.stop_after_attempt(2),
        wait=wait_from_instance,
        reraise=True,
    )
    async def get_categories(self):

        # get category list
        return await self.mm.get_transaction_categories()

    # on a failed login, we need to ignore old auth session and make a new one.
    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            gql.transport.exceptions.TransportServerError
        )
        & tenacity.retry_if_exception(is_exception_401),
        # force ignore old session on 401
        before_sleep=login_before_sleep,
        stop=tenacity.stop_after_attempt(2),
        wait=wait_from_instance,
        reraise=True,
    )
    async def get_transactions(self):

        # TODO we want the category group,
        # TODO we want the transaction tags

        # get transactions dict
        txs = await self.mm.get_transactions(
            limit=RECORD_LIMIT_TX,
            # we always want YTD
            start_date=datetime.date(datetime.date.today().year, 1, 1).isoformat(),
            end_date=datetime.date.today().isoformat(),
        )
        return txs

    async def report_transactions(self):

        cats = await self.get_categories()
        cqs = CategoryQuerySchema()
        catmap = cqs.load(cats)

        txs = await self.get_transactions()

        # # init schema object instance from dict
        schema = TransactionsQuerySchema()
        with TransactionCategorySchemaContext({"catmap": catmap}):
            loaded_data = schema.load(txs)

        # format and write as csv
        self.write_transactions(loaded_data)
        return

    def write_balances(self, accounts):

        with open(self.fn_balance_report, mode="w", newline="", encoding="utf-8") as f:
            # ignore arg will ignore keys not in the header; Account object has extra holdings info not wanted in
            # the output.
            writer = csv.DictWriter(
                f,
                fieldnames=BALANCES_HEADER,
                quoting=csv.QUOTE_ALL,
                extrasaction="ignore",
            )
            writer.writeheader()
            for s in accounts:
                writer.writerow(s.__dict__)

    def write_balances_history(self, accounts):

        with open(
            self.fn_balance_history_report, mode="a", newline="", encoding="utf-8"
        ) as f:
            # ignore arg will ignore keys not in the header; Account object has extra holdings info not wanted in
            # the output.
            writer = csv.DictWriter(
                f,
                fieldnames=BALANCES_HEADER,
                quoting=csv.QUOTE_ALL,
                extrasaction="ignore",
            )
            # If the file pointer is at the start, file is empty or was new and it's safe to write a header.
            # otherwise assume file exists already with content and a header.
            if f.tell() == 0:
                writer.writeheader()
            for s in accounts:
                writer.writerow(s.__dict__)

    async def report_balances(self):
        accounts = await self.get_accounts()

        # # init schema object instance from dict
        schema = AccountsQuerySchema()
        loaded_data = schema.load(accounts)

        # format and write as csv
        self.write_balances(loaded_data)
        self.write_balances_history(loaded_data)

        # portfolio needs subset of this info, don't query it again
        return loaded_data

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            gql.transport.exceptions.TransportServerError
        )
        & tenacity.retry_if_exception(is_exception_401),
        # force ignore old session on 401
        before_sleep=login_before_sleep,
        stop=tenacity.stop_after_attempt(2),
        wait=wait_from_instance,
        reraise=True,
    )
    async def get_accounts(self):
        return await self.mm.get_accounts()

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            gql.transport.exceptions.TransportServerError
        )
        & tenacity.retry_if_exception(is_exception_401),
        # force ignore old session on 401
        before_sleep=login_before_sleep,
        stop=tenacity.stop_after_attempt(2),
        wait=wait_from_instance,
        reraise=True,
    )
    async def get_holdings(self, account_id):
        return await self.mm.get_account_holdings(account_id)

    # assumes the account field is filled; we don't get that inside the individual holding object, we have to
    # backfill it from the name of the account we used to initiate the holdings query
    def write_portfolio(self, holdings):
        with open(
            self.fn_portfolio_report, mode="w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(
                f, fieldnames=PORTFOLIO_HEADER, quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            for h in holdings:
                writer.writerow(h.__dict__)
        return

    # no retry on the toplevel, let the lower level queries retry themselves
    # if any raise, this does too and no file is written.
    async def report_portfolio(self, accounts_query):

        # probably pass loaded object array into both reports.
        # get a list of account names and IDs
        all_holdings = []

        for account in accounts_query:
            if int(account.holdingsCount) > 0:
                # TODO underlying GQL would allow a list of IDs and fewer HTTP calls!
                # in fact we could probably do the holdings filtering more directly too if we learn the query syntax.
                holdings = await self.get_holdings(account.id)
                hqs = HoldingsQuerySchema()
                holdings_query = hqs.load(holdings)
                for x in holdings_query:
                    x.account = account.account
                all_holdings.extend(holdings_query)

        self.write_portfolio(all_holdings)
        return


# ref args explicitly to facilitate easier unit testing
async def main(argument_list):

    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True, help="Monarch email")
    parser.add_argument("--password", required=True, help="Monarch password")
    parser.add_argument(
        "--token",
        required=True,
        help="Monarch MFA secret, displayed by QR code during MFA setup",
    )
    parser.add_argument("--session", required=False, help="session file to use")

    # allow overriding output locations
    parser.add_argument(
        "--report_balances",
        required=False,
        help="file location for balances report",
        default=FN_BALANCE_REPORT,
    )
    parser.add_argument(
        "--report_balances_history",
        required=False,
        help="file location for balances history report",
        default=FN_BALANCE_HISTORY_REPORT,
    )
    parser.add_argument(
        "--report_transactions",
        required=False,
        help="file location for transactions report",
        default=FN_TX_REPORT,
    )
    parser.add_argument(
        "--report_portfolio",
        required=False,
        help="file location for portfolio holdings report",
        default=FN_PORTFOLIO_REPORT,
    )

    command_args = parser.parse_args(args=argument_list)
    # tool's validator is very poor. fail fast.
    assert command_args.username is not None and len(command_args.username) > 0
    assert command_args.password is not None and len(command_args.password) > 0
    assert command_args.token is not None and len(command_args.token) > 0
    assert (
        command_args.report_balances is not None
        and len(command_args.report_balances) > 0
    )
    assert (
        command_args.report_balances_history is not None
        and len(command_args.report_balances_history) > 0
    )
    assert (
        command_args.report_transactions is not None
        and len(command_args.report_transactions) > 0
    )
    assert (
        command_args.report_portfolio is not None
        and len(command_args.report_portfolio) > 0
    )

    m = Monarch(
        un=command_args.username,
        pw=command_args.password,
        token=command_args.token,
        session_file=command_args.session,
        rb=command_args.report_balances,
        rbh=command_args.report_balances_history,
        rt=command_args.report_transactions,
        rp=command_args.report_portfolio,
    )

    # will use a saved session if available; will write a new session whenever we create it.
    # if a valid but timed-out session is here,
    await m.login()

    # create balances.csv
    accounts_query = await m.report_balances()
    # create transactions.csv
    await m.report_transactions()
    # create portfolio.csv
    await m.report_portfolio(accounts_query)

    return


if __name__ == "__main__":
    # drop the program name from the list of command args
    asyncio.run(main(sys.argv[1:]))

# TODO need a logger.
