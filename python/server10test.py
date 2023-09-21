from boltons.socketutils import BufferedSocket
import socket
sock = socket.create_connection(("vcs.protohackers.com", 30307))
# sock = socket.create_connection(("localhost", 40000))
s = BufferedSocket(sock)


def until_ready():
    while True:
        msg = s.recv_until(b'\n')
        print(f"--> {msg}")
        if msg == b"READY":
            break


def send(msg: bytes):
    s.sendall(msg)
    print(f"<-- {msg}")


until_ready()

# send(b'PUT /Jwxa/F/ 14\n')
# send(b"Hello, world!\n")
# until_ready()

# send(b"PUT /a.txt 14\n")
# send(b"Hello, world!\n")
# until_ready()
#
# send(b"PUT /b/test.txt 19\n")
# send(b"A third text file.\n")
# until_ready()
#
# send(b"PUT /b/foo.txt 19\n")
# send(b"A third text file.\n")
# until_ready()
#
# send(b"PUT /b/d/test.txt 19\n")
# send(b"A third text file.\n")
# until_ready()
#
# send(b"PUT /c.txt 14\n")
# send(b"Hello, world!\n")
# until_ready()
#
# send(b"GET /test.txt r0\n")
# until_ready()
#
# send(b"GET /test.txt r1\n")
# until_ready()

# send(b"PUT /test2.txt 27\n")
# send(b"This is another text file.\n")
# until_ready()
#
send(b'HELP\n')
until_ready()

send(b'HELP HELP HELP\n')
until_ready()
send(b'HELP GET\n')
until_ready()
send(b'HELP PUT\n')
until_ready()
send(b'HELP asdasdadasda\n')
until_ready()
send(b'HELPY asdasdadasda\n')
until_ready()
#
# send(b'LIST /b\n')
# until_ready()
#
# send(b'LIST /x\n')
# until_ready()

s.close()