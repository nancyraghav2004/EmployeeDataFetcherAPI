import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from batch import process_employee_files
import logging

logger = logging.getLogger(__name__)

class FileWatcherHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            if file_path.endswith('.csv'):
                self.process_file(file_path)

    def process_file(self, file_path):
        # Extract the filename without the directory
        file_name = os.path.basename(file_path)
        logger.info(f"Processing file: {file_name}")

        # Initialize file paths
        employee_file_path = None
        family_file_path = None

        #Determine if it's an employee or family file
        if 'Employee' in file_name:
            employee_file_path = file_path
        if 'Emp_family' in file_name:
            family_file_path = file_path


           # Process the files
        if employee_file_path:
            logger.info(f"Processing employee file: {employee_file_path}")
            process_employee_files(employee_file_path, family_file_path)
        elif family_file_path:
            logger.info(f"Processing family file: {family_file_path}")
            process_employee_files(employee_file_path, family_file_path)
        else:
            logger.warning(f"File {file_name} does not match the expected pattern. Skipping.") 

        # # Process the files
        # if employee_file_path or family_file_path:
        #     process_employee_files(employee_file_path, family_file_path)
        # else:
        #     logger.warning(f"File {file_name} does not match the expected pattern. Skipping.")

def start_file_watcher(directory):
    event_handler = FileWatcherHandler()
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()
    try:
        while True:
            # Keep the script running
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    directory_to_watch = 'upload_files'
    start_file_watcher(directory_to_watch)