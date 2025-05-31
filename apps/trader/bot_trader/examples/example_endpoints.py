from apps.trader.bot_trader.public_endpoints import ExchangeClient, ExchangeClientConfig, ExchangeClientError, ValidationError, HTTPRequestError


# Use environment variables:
config = ExchangeClientConfig()
client = ExchangeClient(config)

try:
    # Create a new instrument (ID=100)
    book_resp = client.create_order_book(instrument_id=100)
    print("Book creation:", book_resp)

    # Place a GTC SELL
    sell_resp = client.place_order(
        instrument_id=100,
        side="SELL",
        order_type="GTC",
        price_cents=10000,
        quantity=5
    )
    print("Placed SELL:", sell_resp)

    # Place a crossing BUY @10100×3
    buy_resp = client.place_order(
        instrument_id=100,
        side="BUY",
        order_type="GTC",
        price_cents=10100,
        quantity=3
    )
    print("Placed BUY:", buy_resp)

    # Cancel the remaining SELL
    remaining_oid = sell_resp["order_id"]
    cancel_resp = client.cancel_order(instrument_id=100, order_id=remaining_oid)
    print("Cancel result:", cancel_resp)

except ValidationError as ve:
    print("Validation error:", ve.details)
except HTTPRequestError as he:
    print("HTTP error:", he)
except ExchangeClientError as ce:
    print("Client error:", ce)
