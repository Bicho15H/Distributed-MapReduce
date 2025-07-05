import socket
import time

servers = []
port = 8000

def read_machine(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines() if line.strip()]

def main():
    global servers

    servers = read_machine("machines.txt")

    shuffle_time = 0
    computation_time = 0
    controller_communication_time = 0

    start, local_start = time.time(), time.time()

    client_sockets, outputs, inputs = [], [], []
    for server in servers:
        s = socket.create_connection((server, port))
        client_sockets.append(s)
        outputs.append(s.makefile('w'))
        inputs.append(s.makefile('r'))

    try:
        for i, out in enumerate(outputs):
            msg = ""
            for idx, server in enumerate(servers):
                msg += f"{idx} {server}"
                msg += " 1" if idx == i else " 0"
                if idx != len(servers) - 1:
                    msg += ";"
            out.write(msg + "\n")
            out.flush()

        end = time.time()
        controller_communication_time += (end - start) * 1000
        local_end = time.time()
        print(f"Time for sending node info: {int((local_end - local_start) * 1000)} ms.")

        start, local_start = time.time(), time.time()

        for out in outputs:
            out.write("SPLIT\n")
            out.flush()

        responses = [""] * len(servers)
        while True:
            for i, inp in enumerate(inputs):
                responses[i] = inp.readline().strip()
            response = check_responses(responses)

            if response == "SPLIT_END":
                end, local_end = time.time(), time.time()
                computation_time += (end - start) * 1000
                print(f"Time for SPLIT: {int((local_end - local_start) * 1000)} ms.")
                start, local_start = time.time(), time.time()
                for out in outputs:
                    out.write("SHUFFLE\n")
                    out.flush()

            elif response == "SHUFFLE_END":
                local_end = time.time()
                print(f"Time for SHUFFLE: {int((local_end - local_start) * 1000)} ms.")
                local_start = time.time()
                for out in outputs:
                    out.write("SYNCHRONIZE\n")
                    out.flush()

            elif response == "SYNCHRONIZE_END":
                end, local_end = time.time(), time.time()
                shuffle_time += (end - start) * 1000
                print(f"Time for SYNCHRONIZE: {int((local_end - local_start) * 1000)} ms.")
                start, local_start = time.time(), time.time()
                for out in outputs:
                    out.write("GROUP\n")
                    out.flush()

            elif response == "GROUP_END":
                local_end = time.time()
                print(f"Time for GROUP: {int((local_end - local_start) * 1000)} ms.")
                local_start = time.time()
                for out in outputs:
                    out.write("REDUCE\n")
                    out.flush()

            elif response == "REDUCE_END":
                end, local_end = time.time(), time.time()
                computation_time += (end - start) * 1000
                print(f"Time for REDUCE: {int((local_end - local_start) * 1000)} ms.")
                start, local_start = time.time(), time.time()

                responses = [inp.readline().strip() for inp in inputs]
                max_val, min_val = -float('inf'), float('inf')
                for resp in responses:
                    res = resp.split(";")
                    if res[0] == "0": continue
                    max_val, min_val = max(max_val, int(res[0])), min(min_val, int(res[1]))
                range_val = round((max_val - min_val + 1) / len(servers))
                global_min, msg = min_val, ""
                for i in range(len(servers) - 1):
                    min_val = max_val - range_val + 1
                    if min_val <= global_min or max_val <= global_min:
                        msg += f"{i},0,0;"
                        continue
                    msg += f"{i},{max_val},{min_val};"
                    max_val = min_val - 1
                msg += f"{len(servers)-1},{max_val},{global_min};"
                for out in outputs:
                    out.write(msg + "\n")
                    out.flush()

            elif response == "RANGE_END":
                end, local_end = time.time(), time.time()
                controller_communication_time += (end - start) * 1000
                print(f"Time for RANGE: {int((local_end - local_start) * 1000)} ms.")
                start, local_start = time.time(), time.time()
                for out in outputs:
                    out.write("SHUFFLE2\n")
                    out.flush()

            elif response == "SHUFFLE2_END":
                local_end = time.time()
                print(f"Time for SHUFFLE2: {int((local_end - local_start) * 1000)} ms.")
                local_start = time.time()
                for out in outputs:
                    out.write("SYNCHRONIZE2\n")
                    out.flush()

            elif response == "SYNCHRONIZE2_END":
                end, local_end = time.time(), time.time()
                shuffle_time += (end - start) * 1000
                print(f"Time for SYNCHRONIZE2: {int((local_end - local_start) * 1000)} ms.")
                start, local_start = time.time(), time.time()
                for out in outputs:
                    out.write("GROUP2\n")
                    out.flush()

            elif response == "GROUP2_END":
                end, local_end = time.time(), time.time()
                computation_time += (end - start) * 1000
                print(f"Time for GROUP2: {int((local_end - local_start) * 1000)} ms.")
                for out in outputs:
                    out.write("QUIT\n")
                    out.flush()
                    time.sleep(0.05)

            elif response == "END":
                print("All processes are done.")
                print(f"Time for shuffle: {int(shuffle_time)} ms.")
                print(f"Time for computation: {int(computation_time)} ms.")
                print(f"Time for controller communication: {int(controller_communication_time)} ms.")
                print(f"Time total: {int(shuffle_time + computation_time + controller_communication_time)} ms.")
                break

        for s, o, i in zip(client_sockets, outputs, inputs):
            o.close()
            i.close()
            s.close()

    except Exception as e:
        print(f"Error: {e}")

def check_responses(responses):
    # Checks the responses from all servers and returns the appropriate end signal
    if all("SPLIT_OK" in r for r in responses): return "SPLIT_END"
    if all("SHUFFLE_OK" in r for r in responses): return "SHUFFLE_END"
    if all("SYNCHRONIZE_OK" in r for r in responses): return "SYNCHRONIZE_END"
    if all("GROUP_OK" in r for r in responses): return "GROUP_END"
    if all("REDUCE_OK" in r for r in responses): return "REDUCE_END"
    if all("RANGE_OK" in r for r in responses): return "RANGE_END"
    if all("SHUFFLE2_OK" in r for r in responses): return "SHUFFLE2_END"
    if all("SYNCHRONIZE2_OK" in r for r in responses): return "SYNCHRONIZE2_END"
    if all("GROUP2_OK" in r for r in responses): return "GROUP2_END"
    if all("QUIT_OK" in r for r in responses): return "END"
    return "NO_RESPONSE"

if __name__ == "__main__":
    main()
