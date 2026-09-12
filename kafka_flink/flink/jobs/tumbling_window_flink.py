#Count how many times each fruit appears in 5-second windows:

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Time

env = StreamExecutionEnvironment.get_execution_environment()

data = env.from_collection([
    "gaurav",
    "neha",
    "gaurav",
    "sonu",
    "neha"
])

result = (
    data
    .map(lambda x: (x, 1))
    .key_by(lambda x: x[0])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .reduce(lambda a, b: (a[0], a[1] + b[1]))
)

result.print()
env.execute("Flink Window Example")