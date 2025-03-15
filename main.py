from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from datetime import datetime, timedelta
from collections import Counter
import random
import csv
import io
import os

# Get the absolute path of the current directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Initialize Flask app with explicit template and static folder paths
app = Flask(__name__,
            template_folder=os.path.join(BASE_DIR, 'templates'),
            static_folder=os.path.join(BASE_DIR, 'static'))

# Use environment variable for secret key with a fallback
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-please-change-in-production')
# Disable debug mode in production
app.debug = os.environ.get('FLASK_ENV') == 'development'

# Store submissions in memory (replace with database in production)
leads = []

def generate_sample_data(num_samples=15):
    """Generate simulated building permit data."""
    project_types = ['Residential', 'Commercial', 'Industrial', 'Renovation']
    locations = [
        'Toronto, ON', 'Vancouver, BC', 'Montreal, QC', 'Calgary, AB',
        'Ottawa, ON', 'Edmonton, AB', 'Winnipeg, MB', 'Quebec City, QC',
        'Hamilton, ON', 'Victoria, BC', 'Halifax, NS', 'Saskatoon, SK'
    ]
    value_ranges = [
        'CAD $0 - $50,000',
        'CAD $50,000 - $100,000',
        'CAD $100,000 - $500,000',
        'CAD $500,000 - $1,000,000',
        'CAD $1,000,000+'
    ]

    # Generate random dates within the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    sample_data = []
    for _ in range(num_samples):
        random_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        lead = {
            'project_type': random.choice(project_types),
            'location': random.choice(locations),
            'value_range': random.choice(value_ranges),
            'submission_date': random_date
        }
        sample_data.append(lead)
    
    # Sort by submission date, most recent first
    return sorted(sample_data, key=lambda x: x['submission_date'], reverse=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        project_type = request.form.get('project_type')
        location = request.form.get('location')
        value_range = request.form.get('value_range')

        if not all([project_type, location, value_range]):
            flash('All fields are required!', 'error')
            return redirect(url_for('index'))

        # Create a new lead with CAD prefix
        lead = {
            'project_type': project_type,
            'location': location,
            'value_range': f"CAD {value_range}",
            'submission_date': datetime.now()
        }
        leads.append(lead)
        flash('Lead submitted successfully!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    global leads
    
    # If no leads exist, generate sample data
    if not leads:
        leads = generate_sample_data()

    # Get filter parameters
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    value_range_filter = request.args.get('value_range')

    # Apply filters
    filtered_leads = leads
    if project_type_filter:
        filtered_leads = [lead for lead in filtered_leads if lead['project_type'] == project_type_filter]
    if location_filter:
        filtered_leads = [lead for lead in filtered_leads if lead['location'] == location_filter]
    if value_range_filter:
        filtered_leads = [lead for lead in filtered_leads if lead['value_range'].replace('CAD ', '') == value_range_filter]

    # Calculate statistics based on filtered leads
    total_leads = len(filtered_leads)
    
    # Extract value ranges and calculate total value (using minimum values)
    total_value = 0
    for lead in filtered_leads:
        # Remove 'CAD' prefix and handle the rest of the string
        value_str = lead['value_range'].replace('CAD ', '').split('-')[0].strip('$').replace(',', '')
        # Handle the special case of "1000000+"
        if value_str.endswith('+'):
            value_str = value_str[:-1]  # Remove the '+' symbol
        total_value += int(value_str)
    
    # Count project types from filtered leads
    project_types = Counter(lead['project_type'] for lead in filtered_leads)
    
    # Get unique values for filter dropdowns
    available_filters = {
        'project_types': sorted(set(lead['project_type'] for lead in leads)),
        'locations': sorted(set(lead['location'] for lead in leads)),
        'value_ranges': sorted(set(lead['value_range'].replace('CAD ', '') for lead in leads))
    }
    
    return render_template('dashboard.html',
                         leads=filtered_leads,
                         total_leads=total_leads,
                         total_value=f"CAD ${total_value:,}",
                         project_types=dict(project_types),
                         available_filters=available_filters,
                         request=request)

@app.route('/download-csv')
def download_csv():
    # Get filter parameters
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    value_range_filter = request.args.get('value_range')

    # Apply filters (same logic as dashboard route)
    filtered_leads = leads
    if project_type_filter:
        filtered_leads = [lead for lead in filtered_leads if lead['project_type'] == project_type_filter]
    if location_filter:
        filtered_leads = [lead for lead in filtered_leads if lead['location'] == location_filter]
    if value_range_filter:
        filtered_leads = [lead for lead in filtered_leads if lead['value_range'].replace('CAD ', '') == value_range_filter]

    # Create a StringIO object to write CSV data
    si = io.StringIO()
    writer = csv.writer(si)
    
    # Write headers
    writer.writerow(['Project Type', 'Location', 'Value Range', 'Submission Date'])
    
    # Write data rows
    for lead in filtered_leads:
        writer.writerow([
            lead['project_type'],
            lead['location'],
            lead['value_range'],
            lead['submission_date'].strftime('%Y-%m-%d %H:%M')
        ])
    
    # Create the response
    output = si.getvalue()
    si.close()
    
    # Create a new StringIO object with the output
    mem = io.BytesIO()
    mem.write(output.encode('utf-8'))
    mem.seek(0)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return send_file(
        mem,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'construction_leads_{timestamp}.csv'
    )

@app.route('/api/leads')
def api_leads():
    global leads
    
    # If no leads exist, generate sample data
    if not leads:
        leads = generate_sample_data()
    
    # Get filter parameters from query string
    project_type = request.args.get('project_type')
    location = request.args.get('location')
    
    # Apply filters
    filtered_leads = leads
    if project_type:
        filtered_leads = [lead for lead in filtered_leads if lead['project_type'] == project_type]
    if location:
        filtered_leads = [lead for lead in filtered_leads if lead['location'] == location]
    
    # Format the response data
    response_data = [{
        'project_type': lead['project_type'],
        'location': lead['location'],
        'construction_value': lead['value_range'],
        'permit_date': lead['submission_date'].strftime('%Y-%m-%d %H:%M'),
    } for lead in filtered_leads]
    
    # Return JSON response with metadata
    return jsonify({
        'status': 'success',
        'total_results': len(response_data),
        'filters_applied': {
            'project_type': project_type,
            'location': location
        },
        'data': response_data
    })

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)