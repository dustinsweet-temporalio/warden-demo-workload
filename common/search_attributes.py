"""Custom Search Attributes for the delivery fleet: the visibility demo surface.

One typed vocabulary shared by every workflow type, so a single List Filter can
cut across the fleet ("everything in us-west", "every premium order still in
flight") instead of per-type queries.

Two rules shape where values are set:

  - Search Attributes supplied at START time (client start, child start,
    continue-as-new) are NOT billable actions. Everything knowable up front is
    stamped there.
  - Each in-workflow `upsert_search_attributes` call IS one billable action,
    regardless of how many attributes it carries. So upserts are rare,
    deliberate, and batched: an order changes stage three times, settlement
    reports its payout once, a dispatch region reports only when its health
    flips.

Sandbox-safe: only temporalio.common is imported, so workflows can use these
keys directly.
"""
from __future__ import annotations

from temporalio.common import SearchAttributeKey

# --- shared across types -------------------------------------------------
# The region a workflow belongs to. Set on orders, dispatch entities, courier
# shifts, and settlements, so one query slices the whole fleet by geography.
DELIVERY_REGION = SearchAttributeKey.for_keyword("DeliveryRegion")
RESTAURANT_ID = SearchAttributeKey.for_keyword("RestaurantId")
COURIER_ID = SearchAttributeKey.for_keyword("CourierId")

# --- order lifecycle -----------------------------------------------------
# Where an order sits in the saga. The star of the demo: with a region degraded
# (S2), orders visibly pile up in "awaiting_courier".
ORDER_STAGE = SearchAttributeKey.for_keyword("OrderStage")
PRIORITY_TIER = SearchAttributeKey.for_keyword("PriorityTier")
ORDER_VALUE_USD = SearchAttributeKey.for_float("OrderValueUsd")
IS_SURGE_PRICING = SearchAttributeKey.for_bool("IsSurgePricing")
PROMISED_DELIVERY_AT = SearchAttributeKey.for_datetime("PromisedDeliveryAt")
DIETARY_TAGS = SearchAttributeKey.for_keyword_list("DietaryTags")
DELIVERY_NOTES = SearchAttributeKey.for_text("DeliveryNotes")

# --- fleet / dispatch ----------------------------------------------------
# A dispatch region reports its own health and backlog. FLEET_STATUS flips to
# "degraded" exactly when S2 suspends continue-as-new on that region.
FLEET_STATUS = SearchAttributeKey.for_keyword("FleetStatus")
PENDING_ORDER_COUNT = SearchAttributeKey.for_int("PendingOrderCount")
VEHICLE_TYPE = SearchAttributeKey.for_keyword("VehicleType")

# --- batch assignment ----------------------------------------------------
BATCH_MODE = SearchAttributeKey.for_keyword("BatchMode")
FANOUT_SIZE = SearchAttributeKey.for_int("FanoutSize")

# --- settlement ----------------------------------------------------------
COURIER_PAYOUT_USD = SearchAttributeKey.for_float("CourierPayoutUsd")

# Order stage values, in saga order. Only the three marked (*) are upserted
# mid-run; "placed" is stamped at start (free) and there is no terminal
# "delivered" stage because ExecutionStatus = 'Completed' already says that.
STAGE_PLACED = "placed"
STAGE_PREPARING = "preparing"  # *
STAGE_AWAITING_COURIER = "awaiting_courier"  # *
STAGE_IN_TRANSIT = "in_transit"  # *
STAGE_SETTLING = "settling"

# Registration list, used by scripts/register_search_attributes.py. Every key
# defined above belongs here; the script is the only consumer.
ALL_KEYS = [
    DELIVERY_REGION,
    RESTAURANT_ID,
    COURIER_ID,
    ORDER_STAGE,
    PRIORITY_TIER,
    ORDER_VALUE_USD,
    IS_SURGE_PRICING,
    PROMISED_DELIVERY_AT,
    DIETARY_TAGS,
    DELIVERY_NOTES,
    FLEET_STATUS,
    PENDING_ORDER_COUNT,
    VEHICLE_TYPE,
    BATCH_MODE,
    FANOUT_SIZE,
    COURIER_PAYOUT_USD,
]

# Billable actions added per order run by the mid-run upserts above. Used by the
# dashboard's APS estimate so the presenter's number stays honest.
ORDER_STAGE_UPSERT_ACTIONS = 3
