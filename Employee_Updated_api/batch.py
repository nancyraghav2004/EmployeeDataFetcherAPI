import pandas as pd
import logging
from db import insert_employee_data, insert_family_data

logger = logging.getLogger(__name__)

# Function to process employee and family files
def process_employee_files(employee_file_path = None, family_file_path = None):
    logger.info(f"Batch processing started for employee file: {employee_file_path} and family file: {family_file_path}")
    try:
        #Process employee file if available
        if employee_file_path:
            logger.info(f"Processing employee file: {employee_file_path}")
            employee_df = pd.read_csv(employee_file_path)
            employee_df.columns = employee_df.columns.str.strip()

            # Validate and process employee data
            for _, row in employee_df.iterrows():
                insert_employee_data(
                    name=row['Name'],
                    education=row['Education'],
                    joining_year=row['JoiningYear'],  # Use 'JoiningYear' as per your Excel sheet
                    city=row['City'],
                    payment_tier=row['PaymentTier'],  # Use 'PaymentTier' as per your Excel sheet
                    age=row['Age'],
                    gender=row['Gender'],
                    ever_bench=row['EverBenched'],  # Use 'EverBenched' as per your Excel sheet
                    experience=row['ExperienceInCurrentDomain'],  # Use 'ExperienceInCurrentDomain'
                    leave_or_not=row['LeaveOrNot']  # Use 'LeaveOrNot' as per your Excel sheet
                )
            logger.info(f"Employee data from {employee_file_path} processed successfully.")


            # Process family file if available
        if family_file_path:
            logger.info(f"Processing family file: {family_file_path}")
            family_df = pd.read_csv(family_file_path)
            family_df.columns = family_df.columns.str.strip()

            # Validate and process family data
            for _, row in family_df.iterrows():
                insert_family_data(
                    employee_name=row['Name'],
                    family_member_name=row['family_member'],
                    relation=row['relation'],
                    gender=row['gender'],
                    contact=row['contact']
                )
            logger.info(f"Family data from {family_file_path} processed successfully.")
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise