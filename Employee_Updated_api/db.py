import mysql.connector
import logging

logger = logging.getLogger(__name__)

# Establish database connection
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Hello1234",
            database="employee_db"
        )
        logger.info("Database connection established successfully.")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to the database: {str(e)}")
        raise

# Insert employee data
def insert_employee_data(name, education, joining_year, city, payment_tier, age, gender, ever_bench, experience, leave_or_not):
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()
        query = """
        INSERT INTO employees (name, education, joining_year, city, payment_tier, age, gender, ever_bench, experience, leave_or_not)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (name, education, joining_year, city, payment_tier, age, gender, ever_bench, experience, leave_or_not))
        db_connection.commit()
        cursor.close()
        db_connection.close()
        logger.info(f"Employee {name} inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting employee data: {str(e)}")
        raise

# Insert family data
def insert_family_data(employee_name, family_member_name, relation, gender, contact):
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()
        query = """
        INSERT INTO employee_family (employee_name, family_member_name, relation, gender, contact)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (employee_name, family_member_name, relation, gender, contact))
        db_connection.commit()
        cursor.close()
        db_connection.close()
        logger.info(f"Family member {family_member_name} inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting family data: {str(e)}")
        raise

# Fetch all employees
def get_all_employees():
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM employees")
        employees = cursor.fetchall()
        cursor.close()
        db_connection.close()
        return employees
    except Exception as e:
        logger.error(f"Error fetching all employees: {str(e)}")
        raise

# Fetch employee by ID
def get_employee_data(employee_id):
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM employees WHERE id = %s", (employee_id,))
        employee = cursor.fetchone()
        cursor.close()
        db_connection.close()
        return employee
    except Exception as e:
        logger.error(f"Error fetching employee by ID: {str(e)}")
        raise

# Fetch employee family details by employee ID
def get_employee_family_details(employee_name):
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor(dictionary=True)
        query = """
        SELECT family_member_name AS family_member, relation, gender, contact 
        FROM employee_family 
        WHERE employee_name = %s
        """
        cursor.execute(query, (employee_name,))
        family_details = cursor.fetchall()
        cursor.close()
        db_connection.close()
        return family_details
    except Exception as e:
        logger.error(f"Error fetching family details for {employee_name}: {str(e)}")
        raise

# Update employee by ID
def update_employee_by_id(employee_id, data):
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()
        query = """
        UPDATE employees SET name=%s, city=%s, experience=%s WHERE id=%s
        """
        cursor.execute(query, (data['name'], data['city'], data['experience'], employee_id))
        db_connection.commit()
        cursor.close()
        db_connection.close()
        logger.info(f"Employee {employee_id} updated successfully.")
    except Exception as e:
        logger.error(f"Error updating employee: {str(e)}")
        raise
