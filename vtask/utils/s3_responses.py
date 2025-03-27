from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class S3ObjectInfoResponse(BaseModel):
    key: str
    meta: Any = Field(alias="ResponseMetadata")
    accept_ranges: str = Field(alias="AcceptRanges")
    last_modified: datetime = Field(alias="LastModified")
    content_length: int = Field(alias="ContentLength")
    etag: str = Field(alias="ETag")
    content_type: str = Field(alias="ContentType")

    @staticmethod
    def new(d: Any, key: str):
        return S3ObjectInfoResponse(**d, key=key)


class S3ListContentObject(BaseModel):
    key: str = Field(alias="Key")
    last_modified: datetime = Field(alias="LastModified")
    etag: str = Field(alias="ETag")
    size: int = Field(alias="Size")
    storage_class: str = Field(alias="StorageClass")


class S3ListPrefixObject(BaseModel):
    prefix: str = Field(alias="Prefix")


class S3ListResponse(BaseModel):
    meta: Any = Field(alias="ResponseMetadata")
    key_count: int = Field(alias="KeyCount")
    is_truncated: bool = Field(alias="IsTruncated")
    # continuation_token: str | None = Field(alias="ContinuationToken", default=None)
    next_continuation_token: str | None = Field(alias="NextContinuationToken", default=None)
    contents: list[S3ListContentObject] | None = Field(alias="Contents", default=None)
    prefixes: list[S3ListPrefixObject] | None = Field(alias="CommonPrefixes", default=None)

    @staticmethod
    def new(d: Any):
        return S3ListResponse(**d)
