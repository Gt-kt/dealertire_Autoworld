# /app.py
from flask import Flask, render_template, request, send_file, redirect, url_for
import importlib
import io
import pandas as pd

app = Flask(__name__)

# Define your programs. The 'id' must match the script's filename (without .py)
# The 'name' is what users will see on the website.
PROGRAMS = [
    {'id': 'tirepick_daily', 'name': 'Tirepick Daily'},
    {'id': 'weekly_kpi', 'name': 'Weekly KPI'},
    {'id': 'pl_converter', 'name': 'PL_Converter'},
    {'id': 'pl_categorizer', 'name': 'PL_Categorizer'},
    {'id': 'ibx_automation', 'name': 'IBX Automation'},
    {'id': 'crm', 'name': 'CRM'},
    {'id': 'b2c_weekly_p', 'name': 'B2C Weekly_P'},
    {'id': 'margin_by_tire', 'name': 'Margin_by_tire'},
]

PROGRAMS_DICT = {p['id']: p for p in PROGRAMS}

# --- Main Homepage Route ---
@app.route('/')
def index():
    """Renders the homepage with a list of programs."""
    return render_template('index.html', programs=PROGRAMS)


# --- Generic Handler for SIMPLE Programs (1 file in, 1 file out) ---
@app.route('/run/<program_name>', methods=['GET', 'POST'])
def run_program(program_name):
    """Handles file upload for simple, one-file programs."""
    
    # This check redirects any complex programs to their own dedicated route handler.
    # 'b2c_weekly_p' is simple, so it is NOT in this list.
    if program_name in ['ibx_automation', 'crm', 'pl_categorizer', 'tirepick_daily']:
        return redirect(url_for(f'run_{program_name}'))

    if program_name not in PROGRAMS_DICT:
        return "Program not found", 404

    program_info = PROGRAMS_DICT[program_name]

    if request.method == 'POST':
        if 'file' not in request.files or request.files['file'].filename == '':
            return redirect(request.url)
        
        file = request.files['file']
        try:
            module = importlib.import_module(f"scripts.{program_name}")
            # This function name must match the one in the script file.
            output_buffer = module.process_file(file)

            # --- ADDED CHECK ---
            # Check if the processing function returned a valid result.
            if output_buffer is None:
                raise ValueError(f"The '{program_name}' script ran but did not produce an output file. This might happen if the input data was empty or did not meet the script's criteria.")
            # --- END ADDED CHECK ---
            
            output_filename = f"processed_{program_name}_{file.filename}"
            return send_file(
                output_buffer,
                as_attachment=True,
                download_name=output_filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
            # Render the generic upload page with an error message
            return render_template('run_program.html', program_name=program_info['name'], error=str(e))

    # For GET requests, show the generic upload page.
    return render_template('run_program.html', program_name=program_info['name'], error=None)


# --- Dedicated Handler for Tirepick Daily ---
@app.route('/run/tirepick_daily', methods=['GET', 'POST'])
def run_tirepick_daily():
    if request.method == 'POST':
        try:
            file = request.files.get('file')
            analysis_date = request.form.get('analysis_date')

            if not file or not analysis_date:
                raise ValueError("A file and an analysis date are required.")

            module = importlib.import_module("scripts.tirepick_daily")
            result_df = module.analyze_sales_data(file, analysis_date)

            table_html = result_df.to_html(classes='table table-striped', index=False) if not result_df.empty else None
            
            return render_template('view_tirepick_daily_results.html', 
                                   table_html=table_html, 
                                   analysis_date=analysis_date)

        except Exception as e:
            return render_template('run_tirepick_daily.html', error=str(e))
    
    return render_template('run_tirepick_daily.html', error=None)


# --- Dedicated Handler for IBX Automation ---
@app.route('/run/ibx_automation', methods=['GET', 'POST'])
def run_ibx_automation():
    if request.method == 'POST':
        try:
            data_type = request.form.get('data_type')
            sheet_name = request.form.get('sheet_name')
            input_file = request.files.get('input_file')
            template_file = request.files.get('template_file')

            if not all([data_type, sheet_name, input_file, template_file]):
                raise ValueError("All fields are required.")

            module = importlib.import_module("scripts.ibx_automation")
            output_buffer = module.process_files(data_type, sheet_name, input_file, template_file)
            
            output_filename = f"UPDATED_{template_file.filename}"
            return send_file(output_buffer, as_attachment=True, download_name=output_filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        except Exception as e:
            return render_template('run_ibx_automation.html', error=str(e))
    return render_template('run_ibx_automation.html', error=None)


# --- Dedicated Handler for CRM ---
@app.route('/run/crm', methods=['GET', 'POST'])
def run_crm():
    if request.method == 'POST':
        try:
            file1 = request.files.get('file1')
            file2 = request.files.get('file2')
            if not file1 or not file2:
                raise ValueError("Both Dataset 1 and Dataset 2 files are required.")

            module = importlib.import_module("scripts.crm")
            output_buffer = module.process_files(file1, file2)
            
            return send_file(output_buffer, as_attachment=True, download_name='extracted_crm_contacts.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        except Exception as e:
            return render_template('run_crm.html', error=str(e))
    return render_template('run_crm.html', error=None)


# --- Dedicated Handler for P&L Categorizer ---
@app.route('/run/pl_categorizer', methods=['GET', 'POST'])
def run_pl_categorizer():
    if request.method == 'POST':
        try:
            prev_file = request.files.get('prev_file')
            curr_file = request.files.get('curr_file')
            if not prev_file or not curr_file:
                raise ValueError("Both the 'previous' and 'current' month files are required.")

            module = importlib.import_module("scripts.pl_categorizer")
            output_buffer = module.process_files(prev_file, curr_file)
            
            output_filename = f"Categorized_{curr_file.filename}"
            return send_file(output_buffer, as_attachment=True, download_name=output_filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        except Exception as e:
            return render_template('run_pl_categorizer.html', error=str(e))
    return render_template('run_pl_categorizer.html', error=None)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
