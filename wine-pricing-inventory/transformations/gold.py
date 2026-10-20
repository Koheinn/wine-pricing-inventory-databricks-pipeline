import dlt

#Current prices only
dlt.create_streaming_table("gold_current_prices")

dlt.apply_changes(
    target="gold_current_prices",
    source="silver_price_updates",
    keys=["name"],
    sequence_by="effective_date",
    stored_as_scd_type=1
)

#Full price history
dlt.create_streaming_table("gold_price_history")

dlt.apply_changes(
    target="gold_price_history",
    source="silver_price_updates",
    keys=["name"],
    sequence_by="effective_date",
    stored_as_scd_type=2
)