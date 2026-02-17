# expect the volume mount to provide a /app/.env file with these arguments.
# expect a writeable volume called 'out' to be mounted too
. ./env/.env
python monarch.py \
       --username "$MONARCH_EMAIL" \
       --password "$MONARCH_PASSWORD" \
       --token "$MONARCH_TOKEN" \
       --report_balances "./out/balances.csv" \
       --report_transactions "./out/transactions.csv" \
       --report_balances_history "./out/balances_history.csv" \
       --report_portfolio "./out/portfolio.csv"       
