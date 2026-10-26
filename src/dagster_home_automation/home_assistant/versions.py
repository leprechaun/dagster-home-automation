import polars as pl


def _with_removals(snapshots: pl.DataFrame, key: str, tracked: list[str]) -> pl.DataFrame:
    # A member missing from a snapshot taken after it first appeared has been
    # removed. Only snapshots that exist count: an hour with no rows at all
    # has nothing to say about who's missing. A removed row carries the last
    # attributes the member was seen with — copied from the last present row
    # rather than forward_fill()ed, which would also overwrite attributes
    # that were genuinely null while the member was present.
    first_seen = snapshots.group_by(key).agg(pl.col("snapshot_time").min().alias("_first_seen"))
    every_snapshot = first_seen.join(snapshots.select("snapshot_time").unique(), how="cross").filter(
        pl.col("snapshot_time") >= pl.col("_first_seen")
    )
    last_present = (~pl.col("is_removed")).cum_sum().over(key)
    return (
        every_snapshot.drop("_first_seen")
        .join(snapshots.with_columns(pl.lit(False).alias("is_removed")), on=[key, "snapshot_time"], how="left")
        .with_columns(pl.col("is_removed").fill_null(True))
        .sort(key, "snapshot_time")
        .with_columns(last_present.alias("_last_present"))
        .with_columns(*[pl.col(c).first().over(key, "_last_present") for c in [*tracked, "created_at"]])
    )


def build_versions(snapshots: pl.DataFrame, key: str, tracked: list[str]) -> pl.DataFrame:
    # eq_missing, not ==: null == null is null in Polars, which would make an
    # attribute that stays null look like it changed on every snapshot.
    compared = [*tracked, "is_removed"]
    changed = pl.any_horizontal([~pl.col(c).eq_missing(pl.col(c).shift(1).over(key)) for c in compared])
    is_first = pl.col("snapshot_time") == pl.col("snapshot_time").min().over(key)

    return (
        _with_removals(snapshots, key, tracked)
        .with_columns((is_first | changed).cum_sum().over(key).alias("_version"))
        .group_by(key, "_version", maintain_order=True)
        .agg(
            pl.col("created_at").first(),
            pl.col("modified_at").first(),
            pl.col("snapshot_time").min().alias("first_seen"),
            pl.col("snapshot_time").max().alias("last_seen"),
            *[pl.col(c).first() for c in compared],
        )
        # The change happened somewhere in (previous version's last_seen,
        # first_seen]. modified_at is only trusted inside that gap: HA also
        # bumps it for untracked attributes. See docs/adr/0002.
        .with_columns(
            pl.when(pl.col("_version") == 1, pl.col("created_at") <= pl.col("first_seen"))
            .then(pl.col("created_at"))
            .when(pl.col("_version") == 1)
            .then(pl.col("first_seen"))
            .when(pl.col("modified_at").is_between(pl.col("last_seen").shift(1).over(key), pl.col("first_seen"), closed="right"))
            .then(pl.col("modified_at"))
            .otherwise(pl.col("first_seen"))
            .alias("valid_from")
        )
        .with_columns(pl.col("valid_from").shift(-1).over(key).alias("valid_to"))
        .select(key, "valid_from", "valid_to", "last_seen", "is_removed", *tracked)
    )
