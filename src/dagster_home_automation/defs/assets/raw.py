from dagster import AssetSpec, Definitions

_RAW_TABLES = [
    "air-quality",
    "buttons",
    "contact-sensors",
    "electricity",
    "habitat",
    "iot_device_uptime",
    "motion-sensor",
    "multi-presence",
    "presence",
    "smart-bulbs",
    "temperature-and-humidity",
    "zigbee-devices"
]

raw_assets = [
    AssetSpec(
        key=["home-automation", "raw", name],
        group_name="home_automation",
    )
    for name in _RAW_TABLES
]

defs = Definitions(assets=raw_assets)
