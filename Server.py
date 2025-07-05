import socket
import sys
import os
import time
import threading
import hashlib

# ======= Global configuration =======
save_results = False  # Saves the final word counts to a file

# ======= Server configuration =======
server_port = 8000
dataset_directory = "dataset"

# ======= Shared variables =======
id = None # ID of this server
servers_num = None # Total number of servers

# ======= Communication with the controller =======
server_socket = None
out = None
inp = None

# ======= Communication with peers =======
thread_listeners = None
sockets = None
peer_outputs = None

# ======= Shared dicts =======
servers = {} # {server_id: server_address} dict
words_per_server = {} # {server_id: [words]} dict
ranges = {} # {server_id: (max, min)} dict
word_count_list = {} # {word: [counts]} dict
count_word_list = {} # {count: [words]} dict
final_count_word_list = {} # {count: [words]} dict


def split():
    global words_per_server
    print(f"Started SPLIT on server {id}.")

    files = os.listdir(dataset_directory)

    line_num = 0

    for fname in files:
        file_path = os.path.join(dataset_directory, fname)
        if os.path.isfile(file_path) and ".wet" in fname:
            print(f"Splitting file: {fname}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line_num % servers_num == id:
                            tokens = line.strip().split()
                            for token in tokens:
                                server_id = int(hashlib.sha256(token.encode()).hexdigest()[:8], 16) % servers_num
                                words_per_server.setdefault(server_id, []).append(token)
                        line_num += 1
            except Exception as e:
                print(f"ERROR building words_per_server: {e}")

    # Send back response over socket
    try:
        out.write("SPLIT_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")


def shuffle():
    global peer_outputs, thread_listeners

    print(f"Started SHUFFLE on server {id}.")

    # Open listener threads
    thread_listeners = start_thread_listeners(servers_num, server_port, id)
    time.sleep(0.5)

    # Open socket client connections to other peers
    peer_outputs = connect_to_peers(servers_num, server_port, id, servers)

    for svr_idx, word_list in words_per_server.items():
        if svr_idx in servers and svr_idx != id:
            try:
                peer_outputs[svr_idx].write("SHUFFLE;")
                peer_outputs[svr_idx].write(";".join(word_list))
                peer_outputs[svr_idx].write("\n")
                peer_outputs[svr_idx].flush()
                print(f"Server {id} sent {len(word_list)} words to server {svr_idx}")
            except Exception as e:
                print(f"ERROR: server {id} write to peer_outputs error: {e}")

        else:
            for token in word_list:
                word_count_list.setdefault(token,[]).append(1)
            print(f"Server {id} kept {len(word_list)} words locally.")
            

    # Send FINISH signal to other servers to terminate their listener loops
    for i in range(servers_num):
        if i == id:
            continue
        try:
            peer_outputs[i].write("FINISH\n")
            peer_outputs[i].flush()
        except Exception as e:
            print(f"ERROR: server {id}] write to peer_outputs error: {e}")

    # Send back response to controller
    try:
        out.write("SHUFFLE_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")



def synchronize():
    print(f"Started SYNCHRONIZE on server {id}.")
    wait_threads(thread_listeners)

    # send back
    try:
        out.write("SYNCHRONIZE_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")


def group():
    print(f"Started GROUP on server {id}.")

    # group the word counts from all threads
    for thread in thread_listeners:
        if thread is not None:
            for word, counts in thread.get_word_count_list().items():
                word_count_list.setdefault(word,[]).extend(counts)

    # send back
    try:
        out.write("GROUP_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")

def reduce():
    print(f"Started REDUCE on server {id}.")

    # REDUCE the max and min count of each word
    max_count = float('-inf')
    min_count = float('inf')

    for word, counts in word_count_list.items():
        total = sum(counts)
        max_count = max(max_count, total)
        min_count = min(min_count, total)

        # merge into count_word_list: {count: [words]}
        count_word_list.setdefault(total,[]).append(word)

    # handle empty case
    if max_count == float('-inf') or min_count == float('inf'):
        max_count, min_count = 0, 0

    # send back REDUCE_OK
    try:
        out.write("REDUCE_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")

    # send max;min
    try:
        out.write(f"{max_count};{min_count}\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write max/min to out error: {e}")

    # receive max/min ranges
    try:
        line = inp.readline()
        tokens = line.strip().split(';')
        for token in tokens:
            if(token == ""):
                continue
            parts = token.split(',')
            svr_idx = int(parts[0])
            max_val = int(parts[1])
            min_val = int(parts[2])
            ranges[svr_idx]= (max_val, min_val)
    except Exception as e:
        print(f"ERROR: server {id}] reading range error: {e}")

    # send back RANGE_OK
    try:
        out.write("RANGE_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id}] write RANGE_OK error: {e}")

def shuffle2():
    print(f"Started SHUFFLE2 on server {id}.")

    global thread_listeners, peer_outputs
    thread_listeners = start_thread_listeners(servers_num, server_port, id)
    time.sleep(0.5)
    peer_outputs = connect_to_peers(servers_num, server_port, id, servers)

    for count, word_list in count_word_list.items():
        for idx, (max_val, min_val) in ranges.items():
            if min_val <= count <= max_val:
                target_server_idx = idx
                break
        if target_server_idx != id:
            try:
                peer_outputs[target_server_idx].write(f"SHUFFLE2;{count};")
                peer_outputs[target_server_idx].write(";".join(word_list))
                peer_outputs[target_server_idx].write("\n")
                peer_outputs[target_server_idx].flush()
            except Exception as e:
                print(f"ERROR: server {id} flush error: {e}")

        else:
            final_count_word_list.setdefault(count,[]).extend(word_list)


    # send FINISH to end conversations
    for i in range(servers_num):
        if i == id:
            continue
        try:
            peer_outputs[i].write("FINISH\n")
            peer_outputs[i].flush()
        except Exception as e:
            print(f"ERROR: server {id} sent finish error: {e}")

    # send back SHUFFLE2_OK
    try:
        out.write("SHUFFLE2_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write SHUFFLE2_OK error: {e}")

def synchronize2():
    print(f"Started SYNCHRONIZE2 on server {id}.")
    wait_threads(thread_listeners)

    # send back
    try:
        out.write("SYNCHRONIZE2_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")

def group2():
    print(f"Started GROUP2 on server {id}.")

    for thread in thread_listeners:
        if thread is not None:
            for count, words in thread.get_count_word_list().items():
                final_count_word_list.setdefault(count,[]).extend(words)

    # send back
    try:
        out.write("GROUP2_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write to out error: {e}")

def quit():
    if save_results:
        with open("word_count.txt", "a") as f:
            output = ""
            for count, words in sorted(final_count_word_list.items(), reverse=True):
                output += f"{count}: {', '.join(words)}\n"
            f.write(output)

    print(f"Finished on server {id}.")
    try:
        out.write("QUIT_OK\n")
        out.flush()
    except Exception as e:
        print(f"ERROR: server {id} write QUIT_OK error: {e}")

    try:
        out.close()
        inp.close()
        server_socket.close()
        for i in range(servers_num):
            if i == id:
                continue
            sockets[i].close()
            peer_outputs[i].close()
            thread_listeners[i].closeThread()
    except Exception as e:
        print(f"ERROR: server {id} close error: {e}")


def wait_threads(thread_listeners):
    for i in range(servers_num):
        if i == id:
            continue
        try:
            thread_listeners[i].join()
        except Exception as e:
            print(f"ERROR: server {id} Thread {i} join failed: {e}")

    print(f"Server {id}: all threads are ready.")

    # close thread listeners
    for i in range(servers_num):
        if i == id:
            continue
        thread_listeners[i].closeThread()


def connect_to_peers(servers_num, server_port, id, servers):
    global sockets
    peer_outputs = [None] * servers_num

    sockets = [None] * servers_num

    for i in range(servers_num):
        if i == id:
            continue
        try:
            target_port = server_port + id + 1
            s = socket.create_connection((servers[i], target_port))
            sockets[i] = s
            peer_outputs[i] = s.makefile('w')
            print(f"Server {id} connected to server {i} on port {target_port} successfully!")
        except Exception as e:
            print(f"ERROR: server {id} failed to connect to server {i} port {target_port}: {e}")
    return peer_outputs


def start_thread_listeners(servers_num, server_port, id):
    thread_listeners = [None] * servers_num
    for i in range(servers_num):
        if i == id:
            continue
        try:
            port = server_port + i + 1
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('', port))
            listener.listen(1)
            print(f"Server {id} listening on port {port}...")
            l = Listener(listener)
            thread_listeners[i] = l
            l.start()
        except Exception as e:
            print(f"ERROR: server {id} failed to start listener on port {port}: {e}")
    return thread_listeners


class Listener(threading.Thread):
    def __init__(self, listener):
        super().__init__()
        self.listener = listener  # ServerSocket
        self.word_count_list= {}  # {word: [count]} for SHUFFLE
        self.count_word_list = {}  # {count: [words]} for SHUFFLE2

    def get_word_count_list(self):
        return self.word_count_list

    def get_count_word_list(self):
        return self.count_word_list
    
    def closeThread(self):
        try:
            # Force accept() to unblock by connecting to our own listening socket
            dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            dummy_sock.connect(('localhost', self.listener.getsockname()[1]))
            dummy_sock.close()
        except Exception:
            pass
        self.listener.close()

    def run(self):
        try:
            conn, addr = self.listener.accept()
            with conn, conn.makefile('r') as f_in:
                while True:
                    line = f_in.readline()
                    if not line:
                        break
                    tokens = line.strip().split(';')
                    if tokens[0] == "SHUFFLE":
                        for token in tokens[1:]:
                            self.word_count_list.setdefault(token, []).append(1)
                        print(f"Server {id} received {len(tokens)-1} words")
                    
                    elif tokens[0] == "SHUFFLE2":
                        count = int(tokens[1])
                        self.count_word_list.setdefault(count,[]).extend(tokens[2:])

                    elif tokens[0] == "FINISH":
                        break

        except Exception as e:
            print(f"ERROR: server {id} in run Listener class error: {e}")
        finally:
            self.listener.close()

def main():
    global server_socket, inp, out, id, servers_num

    # Start socket server
    try:
        listerner = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listerner.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listerner.bind(('', server_port))
        listerner.listen(1)
        print(f"ServerSocket listener on port: {server_port}")
    except OSError as e:
        print(f"Failed to create listener: {e}")
        sys.exit(1)

    try:
        server_socket, addr = listerner.accept()
        print("ServerSocket accepted a client!")

        inp = server_socket.makefile('r')
        out = server_socket.makefile('w')

        first = True

        while True:
            if first:
                msg = inp.readline()
                if not msg:
                    break
                msg = msg.strip()

                # Parse first message:
                tokens = msg.split(';')
                for t in tokens:
                    parts = t.strip().split()
                    if len(parts) < 2:
                        continue
                    idx, addr = int(parts[0]), parts[1]
                    servers[idx] = addr
                    if len(parts) > 2 and parts[2] == "1":
                        id = idx
                servers_num = len(servers)
                first = False
                continue

            line = inp.readline()
            if not line:
                break
            line = line.strip()

            # Dispatch received command to functions
            if line == "SPLIT":
                split()
            elif line == "SHUFFLE":
                shuffle()
            elif line == "SYNCHRONIZE":
                synchronize()
            elif line == "GROUP":
                group()
            elif line == "REDUCE":
                reduce()
            elif line == "SHUFFLE2":
                shuffle2()
            elif line == "SYNCHRONIZE2":
                synchronize2()
            elif line == "GROUP2":
                group2()
            elif line == "QUIT":
                quit()
                break

    except Exception as e:
        print(f"Error during socket receive: {e}")
    finally:
        if inp: inp.close()
        if out: out.close()
        if server_socket: server_socket.close()
        if listerner: listerner.close()

if __name__ == "__main__":
    main()