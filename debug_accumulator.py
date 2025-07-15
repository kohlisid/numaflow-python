#!/usr/bin/env python3

import asyncio
from collections.abc import AsyncIterable
from datetime import datetime
from pynumaflow.accumulator import Accumulator, Datum, Message
from pynumaflow.shared.asynciter import NonBlockingIterator

class DebugAccumulator(Accumulator):
    def __init__(self, counter):
        self.counter = counter
        print(f"DebugAccumulator initialized with counter: {counter}")

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        print("Handler called, starting to process datums")
        datum_count = 0
        async for datum in datums:
            datum_count += 1
            self.counter += 1
            print(f"Processing datum {datum_count}: keys={datum.keys()}, value={datum.value}, counter={self.counter}")
            msg = f"counter:{self.counter}"
            message = Message(str.encode(msg), keys=datum.keys(), tags=[])
            print(f"Created message: {message}")
            await output.put(message)
            print(f"Message sent to output queue")
        print(f"Handler finished, processed {datum_count} datums")

async def simulate_accumulator():
    print("=== Starting Accumulator Simulation ===")
    
    # Create test datums
    datums = []
    for i in range(5):
        datum = Datum(
            keys=["test_key"],
            value=f"test_message_{i}".encode(),
            event_time=datetime.now(),
            watermark=datetime.now(),
            id_=f"test_id_{i}"
        )
        datums.append(datum)
        print(f"Created datum {i}: keys={datum.keys()}, value={datum.value}")

    # Create accumulator
    accumulator = DebugAccumulator(0)
    
    # Create output queue
    output_queue = NonBlockingIterator()
    
    # Create async iterator from datums
    async def datum_generator():
        for datum in datums:
            print(f"Yielding datum: {datum}")
            yield datum
    
    # Run accumulator handler
    print("\n=== Running Handler ===")
    await accumulator.handler(datum_generator(), output_queue)
    
    # Signal end of stream
    from pynumaflow._constants import STREAM_EOF
    await output_queue.put(STREAM_EOF)
    
    # Read results
    print("\n=== Reading Results ===")
    result_count = 0
    async for msg in output_queue.read_iterator():
        if msg == STREAM_EOF:
            print("Received EOF, stopping")
            break
        result_count += 1
        print(f"Result {result_count}: {msg}")
    
    print(f"Total results received: {result_count}")

if __name__ == "__main__":
    asyncio.run(simulate_accumulator())
