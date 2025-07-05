# Distributed Word Count MapReduce

This project implements a **word count MapReduce** on distributed machines, built completely from scratch using Python. It allows you to run experiments scaling from 1 up to N machines, automatically collect the results, and visualize them with detailed graphs.

## Folder Structure

```text
.
├── dataset/            # Folder to hold dataset files for word count
├── graphs/             # Graphs generated after running draw_graphs.py
├── results/            # Output results from each iteration of the experiment
├── Controller.py       # Controller script to orchestrate the MapReduce process
├── draw_graphs.py      # Script to generate performance graphs from results
├── machines.txt        # List of machine hostnames to be used in experiments
├── README.md           # Readme
├── run.sh              # Bash script to deploy and run the experiment end-to-end
└── Server.py           # Server script run on each machine to perform the MapReduce computation
```

## How it Works

1. **Machines File**  
   The `machines.txt` file should list, one per line, the hostnames of the machines that the controller will connect to (e.g., `tp-1a226-02`).

2. **Running the Experiment**  
   Execute the `run.sh` script from your local machine:
   ```bash
   bash run.sh
   ```
   - It will:
     - Write the `machines.txt` file for each iteration (adding one machine at a time from 1 to N).
     - Upload `Server.py` and `Controller.py` to the respective machines.
     - Start the server scripts on the listed machines.
     - Run the controller script on the controller machine.
     - Collect output time results in the `results/` directory.

3. **Generating Graphs**  
   After all iterations are complete, run the graph generation script:
   ```bash
   python3 draw_graphs.py
   ```
   This will process the time results in `results/` and save performance graphs in the `graphs/` directory.

4. **Dataset**
   - Place the dataset you want to analyze in the `dataset/` folder.
   - By default, `run.sh` will use the dataset from a shared location on the remote machines for efficiency:
     ```
     dataset_file="/cal/commoncrawl/CC-MAIN-20230321002050-20230321032050-00486.warc.wet"
     ```

## Configuration

You can customize experiment parameters in `run.sh`:
```bash
N=20                                        # Number of iterations (max number of machines)
computers=(02 03 04 05 ...)                 # List of available machine numbers
login="belhelou-24"                         # SSH login username
controller_machine="tp-1a226-25"            # Machine to run the controller script
local_server_script="Server.py"             # Local path to server script
local_controller_script="Controller.py"     # Local path to controller script
remote_folder="~/Desktop/experiment"        # Remote shared folder for server and controller
dataset_file="/cal/commoncrawl/..."         # Dataset location on remote machines
```

## Notes

- This system is designed for counting words in large datasets using a distributed MapReduce approach.
- Make sure SSH access is properly configured for all machines listed in `machines.txt`.
- The same shared folder is used for both server and controller scripts for efficiency.
