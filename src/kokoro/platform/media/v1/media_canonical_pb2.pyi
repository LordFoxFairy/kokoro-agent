from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CanonicalImageAspectRatio(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CANONICAL_IMAGE_ASPECT_RATIO_UNSPECIFIED: _ClassVar[CanonicalImageAspectRatio]
    CANONICAL_IMAGE_ASPECT_RATIO_SQUARE_1_1: _ClassVar[CanonicalImageAspectRatio]
    CANONICAL_IMAGE_ASPECT_RATIO_LANDSCAPE_4_3: _ClassVar[CanonicalImageAspectRatio]
    CANONICAL_IMAGE_ASPECT_RATIO_LANDSCAPE_16_9: _ClassVar[CanonicalImageAspectRatio]
    CANONICAL_IMAGE_ASPECT_RATIO_PORTRAIT_3_4: _ClassVar[CanonicalImageAspectRatio]
    CANONICAL_IMAGE_ASPECT_RATIO_PORTRAIT_9_16: _ClassVar[CanonicalImageAspectRatio]

class CanonicalImageOutputFormat(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CANONICAL_IMAGE_OUTPUT_FORMAT_UNSPECIFIED: _ClassVar[CanonicalImageOutputFormat]
    CANONICAL_IMAGE_OUTPUT_FORMAT_PNG: _ClassVar[CanonicalImageOutputFormat]
    CANONICAL_IMAGE_OUTPUT_FORMAT_JPEG: _ClassVar[CanonicalImageOutputFormat]
    CANONICAL_IMAGE_OUTPUT_FORMAT_WEBP: _ClassVar[CanonicalImageOutputFormat]
CANONICAL_IMAGE_ASPECT_RATIO_UNSPECIFIED: CanonicalImageAspectRatio
CANONICAL_IMAGE_ASPECT_RATIO_SQUARE_1_1: CanonicalImageAspectRatio
CANONICAL_IMAGE_ASPECT_RATIO_LANDSCAPE_4_3: CanonicalImageAspectRatio
CANONICAL_IMAGE_ASPECT_RATIO_LANDSCAPE_16_9: CanonicalImageAspectRatio
CANONICAL_IMAGE_ASPECT_RATIO_PORTRAIT_3_4: CanonicalImageAspectRatio
CANONICAL_IMAGE_ASPECT_RATIO_PORTRAIT_9_16: CanonicalImageAspectRatio
CANONICAL_IMAGE_OUTPUT_FORMAT_UNSPECIFIED: CanonicalImageOutputFormat
CANONICAL_IMAGE_OUTPUT_FORMAT_PNG: CanonicalImageOutputFormat
CANONICAL_IMAGE_OUTPUT_FORMAT_JPEG: CanonicalImageOutputFormat
CANONICAL_IMAGE_OUTPUT_FORMAT_WEBP: CanonicalImageOutputFormat

class ImageTextToImageSpecV1(_message.Message):
    __slots__ = ("prompt_intent", "aspect_ratio", "candidate_count", "model_option_revision_ref", "output_format")
    PROMPT_INTENT_FIELD_NUMBER: _ClassVar[int]
    ASPECT_RATIO_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_COUNT_FIELD_NUMBER: _ClassVar[int]
    MODEL_OPTION_REVISION_REF_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FORMAT_FIELD_NUMBER: _ClassVar[int]
    prompt_intent: str
    aspect_ratio: CanonicalImageAspectRatio
    candidate_count: int
    model_option_revision_ref: str
    output_format: CanonicalImageOutputFormat
    def __init__(self, prompt_intent: _Optional[str] = ..., aspect_ratio: _Optional[_Union[CanonicalImageAspectRatio, str]] = ..., candidate_count: _Optional[int] = ..., model_option_revision_ref: _Optional[str] = ..., output_format: _Optional[_Union[CanonicalImageOutputFormat, str]] = ...) -> None: ...

class CanonicalMediaOperationInputV1(_message.Message):
    __slots__ = ("contract_major", "definition_revision_ref", "image_text_to_image")
    CONTRACT_MAJOR_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_REVISION_REF_FIELD_NUMBER: _ClassVar[int]
    IMAGE_TEXT_TO_IMAGE_FIELD_NUMBER: _ClassVar[int]
    contract_major: int
    definition_revision_ref: str
    image_text_to_image: ImageTextToImageSpecV1
    def __init__(self, contract_major: _Optional[int] = ..., definition_revision_ref: _Optional[str] = ..., image_text_to_image: _Optional[_Union[ImageTextToImageSpecV1, _Mapping]] = ...) -> None: ...
