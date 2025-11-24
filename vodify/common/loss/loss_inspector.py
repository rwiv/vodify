from pydantic import BaseModel, Field


class InspectResult(BaseModel):
    missing_segments: list[str] = Field(serialization_alias="missingSegments")

    def to_out_dict(self):
        return self.model_dump(by_alias=True, exclude_none=True)


class LossInspector:
    def inspect(self, seg_nums: list[int]):
        return InspectResult(missing_segments=format_ranges(seg_nums))


def format_ranges(nums):
    if not nums:
        return []

    ranges = []
    start = prev = nums[0]

    for num in nums[1:]:
        if num == prev + 1:
            # Continue the current range
            prev = num
        else:
            # End current range
            if start == prev:
                ranges.append(f"1 ({start})")
            else:
                ranges.append(f"{prev-start+1} ({start}-{prev})")
            start = prev = num

    # Handle the final range
    if start == prev:
        ranges.append(f"1 ({start})")
    else:
        ranges.append(f"{prev - start + 1} ({start}-{prev})")

    return ranges
