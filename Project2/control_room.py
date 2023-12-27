# Ahmet Ertugrul Hacioglu 2020400132
# Ceyhun Sonyurek 2020400258
# Group no: 1

#The code is developed and tested in a macOS environment.
import sys
from mpi4py import MPI

if len(sys.argv) < 3:
    print("Please provide input and output file names.")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

file = open(input_file, "r")
machine_num = int(file.readline()) # Read the number of machines
production_cycles = (int(file.readline())) # Read the number of production cycles

wear_factors = file.readline().split()  # Read wear factors for operations
enhance_factor = int(wear_factors[0])  # Set enhance factor
reverse_factor = int(wear_factors[1])  # Set reverse factor
chop_factor = int(wear_factors[2])  # Set chop factor
trim_factor = int(wear_factors[3])  # Set trim factor
split_factor = int(wear_factors[4])  # Set split factor
threshold = int(file.readline())  # Read the threshold value

machine_dict = {}  # Initialize dictionary for machine information
for m in range(machine_num - 1):
    line = file.readline().split()
    id = int(line[0]) - 1  # Get machine ID
    parent = int(line[1]) - 1  # Get parent ID
    operation = line[2]  # Get operation type

    # Set factor based on operation type
    if(operation == "enhance"):
        factor = enhance_factor
    if (operation == "reverse"):
        factor = reverse_factor
    if (operation == "chop"):
        factor = chop_factor
    if (operation == "trim"):
        factor = trim_factor
    if (operation == "split"):
        factor = split_factor

    # Create or update machine information in the dictionary
    if id not in machine_dict:
        machine_dict[id] = {'id':id,'parent': parent, 'operation': operation, 'children': [],'factor': factor,'isLeaf': False,'Product': ""}
    else:
        children = machine_dict[id]['children']
        machine_dict[id] = {'id':id,'parent': parent, 'operation': operation, 'children': children,'factor': factor,'isLeaf': False,'Product': ""}

    # Create or update parent machine information in the dictionary
    if parent not in machine_dict:
        machine_dict[parent] = {'id': parent,'parent': None, 'operation': None, 'children': [],'factor':None,'isLeaf': False,'Product': ""}
    machine_dict[parent]['children'].append(id)  # Add child ID to parent's children list
    machine_dict[parent]['children'].sort() # The concatenation must be done in the order of the child machines id numbers.
leaf_nodes = [node for node, info in machine_dict.items() if not info['children']] # Find the leaf nodes
leaf_nodes.sort()  # Sort the leaf nodes in ascending order

products = []  # Initialize list for products
for i in range(len(leaf_nodes)): # Read lines from file for each leaf node and append to products list
    line = file.readline().rstrip("\n")
    products.append(line)

product_assignments = {}
for i, product in enumerate(products): # Assign products to respective leaf machines based on leaf node IDs
    leaf_machine_id = leaf_nodes[i]
    product_assignments[leaf_machine_id] = product

# Display product assignments to leaf machines and mark them as leaf nodes with their assigned products
for leaf_id, product in product_assignments.items():
    machine_dict[leaf_id]['isLeaf'] = True
    machine_dict[leaf_id]['Product'] = product

comm = MPI.COMM_SELF.Spawn(command="python3", args=["worker.py"], maxprocs=machine_num)  # Spawn MPI processes

f = open(output_file, "w")

# Send data to spawned processes (machines) using MPI communication
for machine_id in range(machine_num):
    sending_data = [machine_num,production_cycles,machine_dict[machine_id],threshold,machine_id, wear_factors]
    comm.send(sending_data, dest=machine_id, tag=1)

counter = 0 # Initialize counter for tracking production cycles

while True: # Receive data from source 0 with tag 1 and write it to the output file
    received_data = comm.recv(source=0, tag=1)
    f.write(str(received_data) + "\n")

    counter += 1  # Increment the counter upon receiving data
    if counter >= production_cycles: # Break the loop when reaching the specified production cycles
        break

# Receive remaining data from any source and write it to the output file
while True:
    if comm.iprobe(source=MPI.ANY_SOURCE):
        received_data = comm.recv(source=MPI.ANY_SOURCE)  # Receive data from any source
        converted_data = [str(item) for item in received_data]  # Convert all elements to strings
        f.write("-".join(converted_data) + "\n")  # Write converted data to file in a specific format
    else:
        break  # Break the loop when no more messages are available
f.close()
file.close()
MPI.Finalize()