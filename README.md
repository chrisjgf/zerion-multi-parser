## Zerion multi-account parser

Export multiple zerion csv exports and combine into one master file ready for use with BittyTax.

Note: I wrote this in a panic of tax haze last year so the code is pretty messy and there may be redundancies in the code (ie if Zerion began to support multi-account exports...). It may need some tweaking to run correctly. Good luck

### Setup

- Install [bittytax](https://github.com/BittyTax/BittyTax)
- Add addresses to `wallets/known.txt`
- Run `python3 parser.py`
- Optionally run `bittytax <zerion.csv>` for your generated tax report

### Information

- This repo doesn't handle history from exchange accounts and is purely for onchain Ethereum mainnet activity.
- Some assumptions are made around Deposit/Withdrawal to reduce errors in BittyTax, see L480 in `zerion.py`. If sender is a known wallet, the transaction is marked as a Deposit & Withdrawal on both sides of the transactions.
- The file `data-checker/analyse.py` can be used if you are encountering errors in your CSV export preventing you from running BittyTax correctly. Enter the last known working line number and increment until it throws.
