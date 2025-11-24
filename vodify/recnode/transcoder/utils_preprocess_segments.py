from aiofiles import os as aios
from pydantic import BaseModel, Field
from pyutils import path_join

from ...utils import stem


async def _get_deduplicated_seg_paths(extract_seg_paths: list[str]):
    seg_map: dict[int, str] = {}
    mismatch_seg_infos = []
    for seg_path in extract_seg_paths:
        seg_num = int(stem(seg_path))
        if seg_num not in seg_map:
            seg_map[seg_num] = seg_path
        else:
            prev_size = (await aios.stat(seg_map[seg_num])).st_size
            cur_size = (await aios.stat(seg_path)).st_size
            if prev_size != cur_size:
                mismatch_seg_infos.append({"path1": seg_map[seg_num], "path2": seg_path})
    return list(seg_map.values()), mismatch_seg_infos


async def _get_sorted_segment_paths(segments_path: str) -> list[str]:
    paths = []
    for filename in await aios.listdir(segments_path):
        if filename.endswith(".ts"):
            paths.append(path_join(segments_path, filename))
    return sorted(paths, key=lambda x: int(stem(x)))


class InspectResult(BaseModel):
    # segment_period: float | None = Field(serialization_alias="segmentPeriod", default=None)
    # loss_ranges: list[str] = Field(serialization_alias="lossRanges")
    # total_loss_time: str = Field(serialization_alias="totalLossTime")
    missing_segments: list[int] = Field(serialization_alias="missingSegments")

    def to_out_dict(self):
        return self.model_dump(by_alias=True, exclude_none=True)


def _check_missing_segments(segment_paths: list[str]) -> InspectResult:
    seg_nums = [int(stem(path)) for path in segment_paths]
    if -1 in seg_nums and seg_nums[0] != -1:  # check if segment sorted order is valid
        raise ValueError(f"Invalid segment sorted order: {segment_paths}")

    seg_nums = set([int(stem(path)) for path in segment_paths])
    missing_seg_nums: list[int] = []
    start_num = int(stem(segment_paths[0]))
    if start_num in (0, -1) and len(segment_paths) > 0:
        start_num = int(stem(segment_paths[1]))
    end_num = int(stem(segment_paths[-1]))
    for cur_num in range(start_num, end_num + 1):
        if cur_num not in seg_nums:
            missing_seg_nums.append(cur_num)

    return InspectResult(
        missing_segments=missing_seg_nums,
    )
