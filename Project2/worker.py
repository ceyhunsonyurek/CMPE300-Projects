# Ahmet Ertugrul Hacioglu 2020400132
# Ceyhun Sonyurek 2020400258
# Group no: 1

#The code is developed and tested in a macOS environment.
from mpi4py import MPI
# Get MPI communicator and information about the current process
comm = MPI.Comm.Get_parent()
node_comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# Receive data from the parent process
data = comm.recv(source=0, tag=1)
# Data structure received in a form: [machine_num, production_cycles, machine_dict[machine_id], threshold, machine_id]

# Extract necessary information from the received data
machine_num = int(data[0])
production_cycles = int(data[1])
production_cycle_initial = int(data[1])  # Store initial production cycle value
threshold = int(data[3])
id = int(data[4])  # Extract machine ID
wear_factors = data[5]  # Extract wear factors
enhance_factor = int(wear_factors[0])
reverse_factor = int(wear_factors[1])
chop_factor = int(wear_factors[2])
trim_factor = int(wear_factors[3])
split_factor = int(wear_factors[4])
accumulated_wear = 0

# Extract machine information from the received dictionary
machine_dict = data[2]
parent = machine_dict['parent']
operation = machine_dict['operation']
children = machine_dict['children']
factor = machine_dict['factor']
isLeaf = machine_dict['isLeaf']
product = machine_dict['Product']

if id == 0:
    # For the machine with ID 1
    while production_cycles>0:
        received_data_str = ""  # String to accumulate received data
        if len(children) == 0:
            received_data_str = product
        else:
            for child in children:
                received_data = node_comm.recv(source=child, tag=1)
                received_data_str += str(received_data)  # Append received data to the string
        # Send accumulated data to the root process (id=0)
        comm.send(received_data_str,dest = 0,tag = 1)
        production_cycles -= 1
elif (id+1) % 2 == 0:
    # For machines with even IDs
    while production_cycles > 0:
        received_data_str = ""  # String to accumulate received data
        if len(children) == 0:
            received_data_str = product
        else:
            for child in children:
                received_data = node_comm.recv(source=child, tag=1) # Receive data from each child using node_communicator and tag 1
                received_data_str += str(received_data)   # Append received data converted to string to the string variable
        # Perform operations based on the machine's operation type
        if operation == "enhance":
            received_data_str = received_data_str[0] + received_data_str + received_data_str[-1]
            accumulated_wear += enhance_factor
            if accumulated_wear >= threshold:
                cost = (accumulated_wear - threshold + 1)*enhance_factor
                # Send information about excessive wear to the control room
                comm.send([id+1, cost, production_cycle_initial-production_cycles+1], dest=0)
                accumulated_wear = 0
            operation = "split"
        elif operation == "chop":
            if len(received_data_str) > 1:
                received_data_str = received_data_str[:-1]
            accumulated_wear += chop_factor
            if accumulated_wear >= threshold:
                cost = (accumulated_wear - threshold + 1)*chop_factor
                # Send information about excessive wear to the root process
                comm.send([id+1, cost, production_cycle_initial-production_cycles+1], dest=0)
                accumulated_wear = 0
            operation = "enhance"
        elif operation == "split":
            str_len = len(received_data_str)
            if str_len > 1:
                received_data_str = received_data_str[:(str_len // 2) * -1]
            accumulated_wear += split_factor
            if accumulated_wear >= threshold:
                cost = (accumulated_wear - threshold + 1)*split_factor
                comm.send([id+1, cost, production_cycle_initial-production_cycles+1], dest=0)
                accumulated_wear = 0
            operation = "chop"
        # Send processed data to the parent node
        node_comm.send(received_data_str, dest=parent,tag=1)
        production_cycles -= 1
elif (id+1) % 2 == 1:
    while production_cycles > 0:
        received_data_str = ""  # String to accumulate received data
        # Check if the current machine has no children (leaf node)
        if len(children) == 0:
            received_data_str = product  # Set the received_data_str to the product if there are no children
        else:
            for child in children:
                received_data = node_comm.recv(source=child, tag=1)
                received_data_str += str(received_data)  # Append received data to the string

        # Perform operations based on the machine's operation type
        if operation == "reverse":
            received_data_str = received_data_str[::-1]
            accumulated_wear += reverse_factor
            if accumulated_wear >= threshold:
                cost = (accumulated_wear - threshold + 1)*reverse_factor
                comm.send([id+1, cost, production_cycle_initial-production_cycles+1], dest=0)
                accumulated_wear = 0
            operation = "trim"
        elif operation == "trim":
            if len(received_data_str) > 2:
                received_data_str = received_data_str[1:-1]
            accumulated_wear += trim_factor
            if accumulated_wear >= threshold:
                cost = (accumulated_wear - threshold + 1)*trim_factor
                comm.send([id+1, cost, production_cycle_initial-production_cycles+1], dest=0)
                accumulated_wear = 0
            operation = "reverse"
        node_comm.send(received_data_str, dest=parent,tag=1)  # Send processed data to the parent node using node_communicator and tag 1
        production_cycles -= 1