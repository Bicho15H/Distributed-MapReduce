import os
import re
import matplotlib.pyplot as plt
import pandas as pd

# --------- CONFIG ---------
logs_folder = "./results"  # your results folder path
save_folder = "./graphs"  # folder to save graphs

phase_keys = [
    "Time for SPLIT", "Time for SHUFFLE", "Time for SYNCHRONIZE",
    "Time for GROUP", "Time for REDUCE", "Time for RANGE",
    "Time for SHUFFLE2", "Time for SYNCHRONIZE2", "Time for GROUP2",
    "Time for sending node info",
]
summary_keys = [
    "Time for shuffle", "Time for computation", "Time for controller communication", "Time total",
]

# --------- EXTRACT DATA ---------
data = []
log_files = sorted(os.listdir(logs_folder), key=lambda x: int(re.search(r'\d+', x).group()))  # sort by number in filename

for idx, fname in enumerate(log_files):
    filepath = os.path.join(logs_folder, fname)
    times = {"Machines": idx + 1}
    with open(filepath) as f:
        for line in f:
            match = re.match(r"(.+?):\s+(\d+)\s+ms", line.strip())
            if match:
                key, val = match.groups()
                times[key.strip()] = int(val)
    data.append(times)

df = pd.DataFrame(data).set_index("Machines")
df = df.fillna(0)  # ensure missing phases show as 0

print(df)  # optional: check extracted data

machines = df.index

# --------- GRAPH 1: Total time vs number of machines ---------
plt.figure()
plt.plot(machines, df["Time total"], marker='o', label="Measured Total Time")
plt.xticks(machines)
plt.xlabel("Number of Machines")
plt.ylabel("Total Time (ms)")
plt.title("Total Execution Time vs Number of Machines")
plt.grid(True)
plt.savefig(f"{save_folder}/graph_total_time.png")

# --------- GRAPH 2: Speedup vs number of machines ---------
ideal_time = df["Time total"].iloc[0]
speedup = ideal_time / df["Time total"]
ideal_speedup = machines  # linear ideal speedup
plt.figure()
plt.plot(machines, speedup, marker='o', label="Measured Speedup")
plt.plot(machines, ideal_speedup, '--', label="Ideal Speedup")
plt.xticks(machines)
plt.xlabel("Number of Machines")
plt.ylabel("Speedup")
plt.title("Speedup vs Number of Machines")
plt.legend()
plt.grid(True)
plt.savefig(f"{save_folder}/graph_speedup.png")

# --------- GRAPH 3: Stacked bar of absolute times for phases ---------
phases = [key for key in phase_keys if key in df.columns]
df_plot = df[phases]

df_plot.plot(kind='bar', stacked=True, figsize=(10,6))
plt.xlabel("Number of Machines")
plt.ylabel("Phase Time (ms)")
plt.title("Absolute Phase Times vs Number of Machines (Stacked Bar)")
plt.xticks(range(len(machines)), machines)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.grid(True, axis='y')
plt.savefig(f"{save_folder}/graph_phase_times_stacked.png")

# --------- GRAPH 4: Stacked bar for absolute times of shuffle, computation, controller communication ---------
special_keys = ["Time for shuffle", "Time for computation", "Time for controller communication"]
df_special = df[special_keys]

df_special.plot(kind='bar', stacked=True, figsize=(10,6))
plt.xlabel("Number of Machines")
plt.ylabel("Time (ms)")
plt.title("Shuffle, Computation, Controller Communication Times (Stacked Bar)")
plt.xticks(range(len(machines)), machines)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.grid(True, axis='y')
plt.savefig(f"{save_folder}/graph_summary_times_stacked.png")

print("Graphs generated:\n - graph_total_time.png\n - graph_speedup.png\n - graph_phase_times_stacked.png\n - graph_summary_times_stacked.png")
