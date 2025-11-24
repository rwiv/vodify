from ...utils import stem

TAR_SIZE_MB = 18
SEG_SIZE_MB = 2


def _get_video_size_by_cnt(size_limit_gb: int, paths: list[str]) -> tuple[bool, float]:
    tars_size_sum_mb = len(paths) * TAR_SIZE_MB
    tars_size_sum_b = tars_size_sum_mb * 1024 * 1024
    tars_size_sum_gb = round(tars_size_sum_b / 1024 / 1024 / 1024, 2)
    is_too_large = tars_size_sum_b > (size_limit_gb * 1024 * 1024 * 1024)
    return is_too_large, tars_size_sum_gb


def _get_video_size_by_name(size_limit_gb: int, paths: list[str]) -> tuple[bool, float]:
    tars_size_sum_mb = 0
    for path in paths:
        stem_name = stem(path)
        if len(stem_name.split("_")) == 3:
            tars_size_sum_mb += TAR_SIZE_MB
        elif len(stem_name.split("_")) == 2:
            tars_size_sum_mb += SEG_SIZE_MB
        else:
            raise ValueError(f"Invalid tar name: {stem_name}")
    tars_size_sum_b = tars_size_sum_mb * 1024 * 1024
    tars_size_sum_gb = round(tars_size_sum_b / 1024 / 1024 / 1024, 2)
    is_too_large = tars_size_sum_b > (size_limit_gb * 1024 * 1024 * 1024)
    return is_too_large, tars_size_sum_gb
