from flask import Flask, request, jsonify
from flask_swagger_ui import get_swaggerui_blueprint
import logging
from flasgger import Swagger
import stat
import os
from threading import Thread
from file_watcher import start_file_watcher
from db import get_all_employees, get_employee_data, update_employee_by_id, get_employee_family_details

app = Flask(__name__)
swagger = Swagger(app)

#Swagger UI setup
SWAGGER_URL = '/apidocs'
API_URL = '/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Logging setup
log_file_path = 'app_log.txt'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

#POST method to upload employee and family CSV files
@app.route('/upload-files', methods = ['POST'])
def upload_files():
    """
    Upload Employee and Family CSV files
    ---
    consumes:
      - multipart/form-data
    parameters:
      - name: employee_file
        in: formData
        type: file
        required: false  # Not required to upload both files
        description: The employee CSV file
      - name: family_file
        in: formData
        type: file
        required: false  # Not required to upload both files
        description: The family CSV file
    responses:
      200:
        description: Files uploaded and ready for batch processing
      400:
        description: Invalid files or error in upload
    """
    try:
        employee_file = request.files.get('employee_file') ##retieves the employee file from the form data
        family_file = request.files.get('family_file')

        if not employee_file and not family_file:
            return jsonify({"error": "No file selected"}), 400
        
        # Save files, if the family file is provided, it saves the file in the upload_files directory.
        if employee_file:
            employee_file_path = f'upload_files/{employee_file.filename}'
            employee_file.save(employee_file_path)
        else:
            employee_file_path = None
        
        if family_file:
            family_file_path = f'upload_files/{family_file.filename}'
            family_file.save(family_file_path)
        else:
            family_file_path = None

        #Call batch processing if either file is uploaded
        #

        return jsonify({"message": "Files uploaded and processed successfully"}),200
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        return jsonify ({"error": "An error occurred during file upload"}),500

#GET method to retrieve all employees
@app.route('/employees', methods=['GET'])
def get_all_employees_route():
    """
    Get all employee details
    ---
    responses:
      200:
        description: List of all employees
      500:
        description: Error occurred while fetching employees
    """
    try:
        employee = get_all_employees()
        return jsonify(employee), 200
    except Exception as e:
        logger.error(f"Error fetching all employees: {str(e)}")
        return jsonify({"error": "An error occurred while fetching employee data"}),500

#GET method to retrieve employee by ID
@app.route('/employee/<int:employee_id>', methods=['GET'])
def get_employee_data_route(employee_id):
    """
    Get employee details by ID
    ---
    parameters:
      - name: employee_id
        in: path
        type: integer
        required: true
        description: Employee ID
    responses:
      200:
        description: Employee details
      404:
        description: Employee not found
    """
    try:
        employee = get_employee_data(employee_id)
        if employee:
            return jsonify(employee), 200
        else:
            return jsonify({"error":"Employee not found"}),404
    except Exception as e:
        logger.error(f"Error fetching employee: {str(e)}")
        return jsonify({"error": "An error occurred while fetching employee data"}),500

#PUT method to update employee details by ID
@app.route('/employee/<int:employee_id>', methods=['PUT'])
def update_employee_by_id_route(employee_id):
    """
    Update employee details by ID
    ---
    parameters:
      - name: employee_id
        in: path
        type: integer
        required: true
        description: Employee ID
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            name:
              type: string
            city:
              type: string
            experience:
              type: integer
    responses:
      200:
        description: Employee updated successfully
      404:
        description: Employee not found
    """
    try:
        data = request.json
        update_employee_by_id(employee_id, data)
        return jsonify({"message": f"Employee {employee_id} updated successfully."}), 200
    except Exception as e:
        logger.error(f"Error updatinf employee: {str(e)}")
        return jsonify({"error": "An error occurred while updating employee data"}), 500
    

#GET Method to retrieve employee family details
@app.route('/employee/<int:employee_id>/family', methods=['GET'])
def get_employee_family_details_route(employee_id):
    """
    Get family details for an employee by employee ID
    ---
    parameters:
      - name: employee_id
        in: path
        type: integer
        required: true
        description: Employee ID
    responses:
      200:
        description: Family details for the employee
      404:
        description: Employee or family details not found
    """
    try:
        # First, get the employee details by ID to find the name
        employee = get_employee_data(employee_id)
        if not employee:
            return jsonify({"error": "Employee not found"}), 404
        
        employee_name = employee['name']

        family_details = get_employee_family_details(employee_name)

        if not family_details:
            return jsonify({"error": "Family details not found"}), 404

        return jsonify({"employee_name": employee_name, "family_details": family_details}), 200
    except Exception as e:
        logger.error(f"Error fetching family details: {str(e)}")
        return jsonify({"error": "An error occurred while fetching family details"}), 500
    
if __name__ ==  '__main__':
    if not os.path.exists('upload_files'):
        os.makedirs('upload_files', exist_ok=True)
        os.chmod('upload_files', stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    watcher_thread = Thread(target=start_file_watcher, args=('upload_files',))
    watcher_thread.start()
    app.run(debug=True)




