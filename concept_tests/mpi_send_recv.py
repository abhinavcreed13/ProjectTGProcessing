import math
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    print(size)
    comm.send(4, dest=1)
    comm.send(9, dest=2)
    comm.send(16, dest=3)
    print('Rank 2 output ->',comm.recv(source=2))
    print('Rank 1 output ->',comm.recv(source=1))
    print('Rank 3 output ->',comm.recv(source=3))

if rank == 1:
    val = comm.recv(source=0)
    print('rank 1 processing')
    comm.send(math.sqrt(val),dest=0)

if rank == 2:
    val = comm.recv(source=0)
    print('rank 2 processing')
    comm.send(math.sqrt(val),dest=0)

if rank == 3:
    val = comm.recv(source=0)
    print('rank 3 processing')
    comm.send(math.sqrt(val),dest=0)
