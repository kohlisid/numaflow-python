# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: transform.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0ftransform.proto\x12\x14sourcetransformer.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bgoogle/protobuf/empty.proto"\x94\x01\n\x16SourceTransformRequest\x12\x0c\n\x04keys\x18\x01 \x03(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12.\n\nevent_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12-\n\twatermark\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp"\xc5\x01\n\x17SourceTransformResponse\x12\x45\n\x07results\x18\x01 \x03(\x0b\x32\x34.sourcetransformer.v1.SourceTransformResponse.Result\x1a\x63\n\x06Result\x12\x0c\n\x04keys\x18\x01 \x03(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12.\n\nevent_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0c\n\x04tags\x18\x04 \x03(\t"\x1e\n\rReadyResponse\x12\r\n\x05ready\x18\x01 \x01(\x08\x32\xcb\x01\n\x0fSourceTransform\x12p\n\x11SourceTransformFn\x12,.sourcetransformer.v1.SourceTransformRequest\x1a-.sourcetransformer.v1.SourceTransformResponse\x12\x46\n\x07IsReady\x12\x16.google.protobuf.Empty\x1a#.sourcetransformer.v1.ReadyResponseb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "transform_pb2", _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _globals["_SOURCETRANSFORMREQUEST"]._serialized_start = 104
    _globals["_SOURCETRANSFORMREQUEST"]._serialized_end = 252
    _globals["_SOURCETRANSFORMRESPONSE"]._serialized_start = 255
    _globals["_SOURCETRANSFORMRESPONSE"]._serialized_end = 452
    _globals["_SOURCETRANSFORMRESPONSE_RESULT"]._serialized_start = 353
    _globals["_SOURCETRANSFORMRESPONSE_RESULT"]._serialized_end = 452
    _globals["_READYRESPONSE"]._serialized_start = 454
    _globals["_READYRESPONSE"]._serialized_end = 484
    _globals["_SOURCETRANSFORM"]._serialized_start = 487
    _globals["_SOURCETRANSFORM"]._serialized_end = 690
# @@protoc_insertion_point(module_scope)
