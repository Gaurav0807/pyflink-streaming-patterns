from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.execution_mode import ExecutionMode


env = StreamExecutionEnvironment.get_execution_environment()


#Create a stream
data = env.from_collection([
    "apple",
    "banana",
    "apple",
    "orange",
    "banana"
])


#Process the stream

result = data.map(lambda x:(x,1)) \
            .key_by(lambda x:x[0]) \
            .reduce(lambda a,b: (a[0],a[1]+b[1]))


result.print()

env.execute("Simple Flink Job")
