import unittest
from datetime import datetime, timezone
from collections.abc import Iterator

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.sinker import Responses, Datum, Response, Sinker
from pynumaflow.sinker.proto import sink_pb2


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    results = Responses()
    for msg in datums:
        if "err" in msg.value.decode("utf-8"):
            results.append(Response.as_failure(msg.id, "mock sink message error"))
        else:
            results.append(Response.as_success(msg.id))
    return results


def err_udsink_handler(_: Iterator[Datum]) -> Responses:
    raise RuntimeError("Something is fishy!")


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_err_message():
    msg = bytes("test_mock_err_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        my_servicer = Sinker(udsink_handler)
        services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = sink_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_udsink_err(self):
        my_servicer = Sinker(err_udsink_handler)
        services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            sink_pb2.SinkRequest(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            ),
            sink_pb2.SinkRequest(
                id="test_id_1",
                value=mock_err_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            ),
        ]

        method = self.test_server.invoke_stream_unary(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]
            ),
            invocation_metadata={},
            timeout=1,
        )

        method.send_request(test_datums[0])
        method.send_request(test_datums[1])
        method.requests_closed()

        response, metadata, code, details = method.termination()
        self.assertEqual(2, len(response.results))
        self.assertEqual("test_id_0", response.results[0].id)
        self.assertEqual("test_id_1", response.results[1].id)
        self.assertFalse(response.results[0].success)
        self.assertFalse(response.results[1].success)
        self.assertTrue(response.results[0].err_msg)
        self.assertTrue(response.results[1].err_msg)
        self.assertEqual(code, StatusCode.OK)

    def test_forward_message(self):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            sink_pb2.SinkRequest(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            ),
            sink_pb2.SinkRequest(
                id="test_id_1",
                value=mock_err_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            ),
        ]

        method = self.test_server.invoke_stream_unary(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]
            ),
            invocation_metadata={},
            timeout=1,
        )

        method.send_request(test_datums[0])
        method.send_request(test_datums[1])
        method.requests_closed()

        response, metadata, code, details = method.termination()
        self.assertEqual(2, len(response.results))
        self.assertEqual("test_id_0", response.results[0].id)
        self.assertEqual("test_id_1", response.results[1].id)
        self.assertTrue(response.results[0].success)
        self.assertFalse(response.results[1].success)
        self.assertEqual("", response.results[0].err_msg)
        self.assertEqual("mock sink message error", response.results[1].err_msg)
        self.assertEqual(code, StatusCode.OK)


if __name__ == "__main__":
    unittest.main()
