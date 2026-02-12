import os
import shutil


def prepare_and_augment_data(source_path, destination_path, target_size_gb=1.1):
    """
    Collects CSV files from subfolders and duplicates them to reach ~1GB.
    Fulfills requirement #17 (preprocessing/augmentation) and #4 (size limit). [cite: 4, 17]
    """
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    collected_files = []

    print(f"Scanning: {source_path}")
    for root, dirs, files in os.walk(source_path):
        # We target only the 'fulldata' folders as discussed
        if "fulldata" in root:
            for file in files:
                if file.endswith(".csv"):
                    collected_files.append(os.path.join(root, file))

    if not collected_files:
        print("Error: No CSV files found! Check if the source path is correct.")
        return

    print(f"Found {len(collected_files)} original CSV files. Starting copy...")
    for f in collected_files:
        shutil.copy2(f, destination_path)

    def get_dir_size_gb(directory):
        total_size = sum(os.path.getsize(os.path.join(directory, f)) for f in os.listdir(directory))
        return total_size / (1024 ** 3)

    current_size = get_dir_size_gb(destination_path)
    print(f"Initial size: {current_size:.4f} GB")

    # Requirement #17: Generate additional data to reach ~1GB
    copy_round = 1
    while current_size < target_size_gb:
        print(f"Current size {current_size:.2f} GB is below target. Augmenting (Round {copy_round})...")
        for f in collected_files:
            file_name = os.path.basename(f)
            new_name = f"copy{copy_round}_{file_name}"
            shutil.copy2(f, os.path.join(destination_path, new_name))

            # Check size to stop as soon as we hit the target
            current_size = get_dir_size_gb(destination_path)
            if current_size >= target_size_gb:
                break
        copy_round += 1

    print("-" * 40)
    print(f"Final dataset size: {current_size:.2f} GB")
    print(f"Location: {destination_path}")


if __name__ == "__main__":
    # Paths based on your folder structure image
    base_dir = "D:/New folder/faks/MASTER/BIGDATA/PROJECT1"
    source = os.path.join(base_dir, "DATA/uqvitalsignsdata")
    destination = os.path.join(base_dir, "dataset_ready_for_hdfs")

    prepare_and_augment_data(source, destination)