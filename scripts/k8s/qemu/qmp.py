"""Send one command to the local QEMU monitor."""
import json
import socket
import sys

path, command = sys.argv[1:]
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
    connection.settimeout(5)
    connection.connect(path)
    with connection.makefile("rwb") as stream:
        json.loads(stream.readline())
        for operation in ("qmp_capabilities", command):
            stream.write(json.dumps({"execute": operation}).encode() + b"\n")
            stream.flush()
            while True:
                response = json.loads(stream.readline())
                if "error" in response:
                    sys.exit(response["error"])
                if "return" in response:
                    break
