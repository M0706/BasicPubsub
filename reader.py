import os
import time

class Consumer:
    def __init__(self, partition_id, directory="partitions", offset_dir="offsets"):
        self.partition_id = partition_id
        self.partition_file_path = os.path.join(directory, f"partition_{partition_id}.txt")
        self.offset_file_path = os.path.join(offset_dir, f"offset_{partition_id}.txt")

        # Ensure directories exist
        os.makedirs(directory, exist_ok=True)
        os.makedirs(offset_dir, exist_ok=True)

        # Initialize or load offset
        self.offset = self.load_offset()

    def load_offset(self):
        """Loads the last read offset from file or starts at 0 if no offset file exists."""
        if os.path.exists(self.offset_file_path):
            with open(self.offset_file_path, "r") as f:
                return int(f.read().strip())
        return 0  # Start from the beginning if offset file doesn't exist

    def save_offset(self):
        """Saves the current offset to file."""
        with open(self.offset_file_path, "w") as f:
            f.write(str(self.offset))

    def read_next(self):
        """Reads new messages from the partition file."""
        messages = []
        with open(self.partition_file_path, "r") as f:
            f.seek(self.offset)

            # Read new lines (if any)
            for line in f:
                messages.append(line.strip())

            # Update offset to the new end of file
            self.offset = f.tell()

        # Save updated offset
        self.save_offset()

        return messages

# Running the consumer in a loop
if __name__ == "__main__":
    partition_id = int(input("Enter the partition ID to consume: "))
    consumer = Consumer(partition_id)

    print(f"Starting consumer for partition {partition_id}...")
    try:
        while True:
            new_messages = consumer.read_next()
            if new_messages:
                print(f"Partition {partition_id} - New messages: {new_messages}")
            else:
                print(f"Partition {partition_id} - No new messages.")

            time.sleep(2)  # Poll every 2 seconds for new messages

    except KeyboardInterrupt:
        print("Consumer stopped.")
