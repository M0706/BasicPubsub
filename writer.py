import os

class Partition:
    def __init__(self, partition_id, directory="/Users/manav.garg/Desktop/Python/appendonlyexample/partitions"):
        self.partition_id = partition_id
        self.file_path = os.path.join(directory, f"partition_{partition_id}.txt")

        os.makedirs(directory, exist_ok=True)

        self.file = open(self.file_path, "a")

    def write(self, message):
        self.file.write(f"{message}\n")
        self.file.flush()  # Ensure data is written immediately

    def close(self):
        self.file.close()


n_partitions = int(input("Enter the number of partitions needed"))
partitions = [Partition(i) for i in range(n_partitions)]

try:
    while True:
        # Get the partition and message from user input
        partition_id = input(f"Enter partition number (0 to {n_partitions - 1}, or 'exit' to quit): ")
        if partition_id.lower() == 'exit':
            break

        # Validate the partition number
        if not partition_id.isdigit() or int(partition_id) not in range(n_partitions):
            print("Invalid partition number. Please try again.")
            continue

        message = input("Enter the message to write: ")

        # Write the message to the specified partition
        partitions[int(partition_id)].write(message)
        print(f"Message written to partition {partition_id}.")

finally:
    # Cleanup: close all files
    for partition in partitions:
        partition.close()
