from pyflink.datastream import StreamExecutionEnvironment


env = StreamExecutionEnvironment.get_execution_environment()

# By default Flink chains adjacent operators into one task for performance,
# which collapses the whole pipeline into a single box in the Flink UI.
# Disabling it shows each operator as its own node in the job graph.
env.disable_operator_chaining()


# --- Source ---
data = env.from_collection([
    "apple",
    "banana",
    "apple",
    "orange",
    "banana"
])

# --- Map ---
# transform each element: "apple" -> ("apple", 1)
mapped = data.map(lambda x: (x, 1))

# --- Filter ---
filtered = mapped.filter(lambda x: x[0] != "banana")

# --- Sink ---
filtered.print()

env.execute("Flink JOb")
