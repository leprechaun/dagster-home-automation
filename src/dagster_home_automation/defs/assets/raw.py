from dagster import AssetSpec, Definitions

# Landed in Delta outside this pipeline; Dagster only reads them.
_RAW_TABLES = {
    "air-quality": "Air quality sensor readings",
    "buttons": "Button press events",
    "contact-sensors": "Door/window contact sensor open/close events",
    "electricity": "Power readings (timestamp, area, power)",
    "habitat": "Habitat sensor readings",
    "iot_device_uptime": "IoT device uptime readings",
    "motion-sensor": "Motion sensor events",
    "multi-presence": "Multi-zone presence sensor readings",
    "presence": "Presence sensor readings",
    "smart-bulbs": "Smart bulb state readings",
    "temperature-and-humidity": "Temperature and humidity sensor readings",
    "zigbee-devices": "Zigbee device readings",
}

raw_assets = [
    AssetSpec(
        key=["home-automation", "raw", name],
        group_name="home_automation",
        description=f"{description}, landed in Delta outside this pipeline",
    ).with_io_manager_key("home_automation_io_manager")
    for name, description in _RAW_TABLES.items()
]

defs = Definitions(assets=raw_assets)
