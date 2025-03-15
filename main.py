from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from datetime import datetime, timedelta
from collections import Counter
import random
import csv
import io
import os
import sys
import logging
from models import db, Lead

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the absolute path of the current directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Ensure template and static directories exist
template_dir = os.path.join(BASE_DIR, 'templates')
static_dir = os.path.join(BASE_DIR, 'static')

try:
    os.makedirs(template_dir, exist_ok=True)
    os.makedirs(static_dir, exist_ok=True)
    logger.info(f"Template directory: {template_dir}")
    logger.info(f"Static directory: {static_dir}")
except Exception as e:
    logger.error(f"Error creating directories: {e}")
    sys.exit(1)

# Initialize Flask app with explicit template and static folder paths
app = Flask(__name__,
            template_folder=os.path.join(BASE_DIR, 'templates'),
            static_folder=os.path.join(BASE_DIR, 'static'))

# Configure SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', f'sqlite:///{os.path.join(BASE_DIR, "leads.db")}')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Use environment variable for secret key with a fallback
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-please-change-in-production')
# Disable debug mode in production
app.debug = os.environ.get('FLASK_ENV') == 'development'

# Initialize the database
db.init_app(app)

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
        
        lead = Lead(
            project_type=random.choice(project_types),
            location=random.choice(locations),
            value_range=random.choice(value_ranges),
            submission_date=random_date
        )
        sample_data.append(lead)
    
    return sample_data

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        project_type = request.form.get('project_type')
        location = request.form.get('location')
        value_range = request.form.get('value_range')

        if not all([project_type, location, value_range]):
            flash('All fields are required!', 'error')
            return redirect(url_for('index'))

        # Create a new lead
        lead = Lead(
            project_type=project_type,
            location=location,
            value_range=f"CAD {value_range}",
            submission_date=datetime.now()
        )
        db.session.add(lead)
        db.session.commit()

        flash('Lead submitted successfully!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    # Get filter parameters
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    value_range_filter = request.args.get('value_range')

    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type_filter:
        query = query.filter_by(project_type=project_type_filter)
    if location_filter:
        query = query.filter_by(location=location_filter)
    if value_range_filter:
        query = query.filter(Lead.value_range == f"CAD {value_range_filter}")

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()
    
    # If no leads exist, generate and add sample data
    if not filtered_leads and not any([project_type_filter, location_filter, value_range_filter]):
        sample_leads = generate_sample_data()
        for lead in sample_leads:
            db.session.add(lead)
        db.session.commit()
        filtered_leads = Lead.query.order_by(Lead.submission_date.desc()).all()

    # Calculate statistics
    total_leads = len(filtered_leads)
    
    # Extract value ranges and calculate total value
    total_value = 0
    for lead in filtered_leads:
        value_str = lead.value_range.replace('CAD ', '').split('-')[0].strip('$').replace(',', '')
        if value_str.endswith('+'):
            value_str = value_str[:-1]
        total_value += int(value_str)
    
    # Count project types
    project_types = Counter(lead.project_type for lead in filtered_leads)
    
    # Get unique values for filter dropdowns from all leads
    all_leads = Lead.query.all()
    available_filters = {
        'project_types': sorted(set(lead.project_type for lead in all_leads)),
        'locations': sorted(set(lead.location for lead in all_leads)),
        'value_ranges': sorted(set(lead.value_range.replace('CAD ', '') for lead in all_leads))
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

    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type_filter:
        query = query.filter_by(project_type=project_type_filter)
    if location_filter:
        query = query.filter_by(location=location_filter)
    if value_range_filter:
        query = query.filter(Lead.value_range == f"CAD {value_range_filter}")

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()

    # Create a StringIO object to write CSV data
    si = io.StringIO()
    writer = csv.writer(si)
    
    # Write headers
    writer.writerow(['Project Type', 'Location', 'Value Range', 'Submission Date'])
    
    # Write data rows
    for lead in filtered_leads:
        writer.writerow([
            lead.project_type,
            lead.location,
            lead.value_range,
            lead.submission_date.strftime('%Y-%m-%d %H:%M')
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
    # Get filter parameters from query string
    project_type = request.args.get('project_type')
    location = request.args.get('location')
    
    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type:
        query = query.filter_by(project_type=project_type)
    if location:
        query = query.filter_by(location=location)

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()
    
    # Format the response data
    response_data = [{
        'project_type': lead.project_type,
        'location': lead.location,
        'construction_value': lead.value_range,
        'permit_date': lead.submission_date.strftime('%Y-%m-%d %H:%M'),
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

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'template_dir': os.path.exists(template_dir),
        'static_dir': os.path.exists(static_dir),
        'database': 'connected' if db.engine.execute('SELECT 1').scalar() else 'error'
    })

# Create database tables before first request
@app.before_first_request
def create_tables():
    db.create_all()

if __name__ == '__main__':
    # Create the database tables
    with app.app_context():
        db.create_all()
    
    # Log startup information
    logger.info(f"Starting application in {os.environ.get('FLASK_ENV', 'production')} mode")
    logger.info(f"Template directory exists: {os.path.exists(template_dir)}")
    logger.info(f"Static directory exists: {os.path.exists(static_dir)}")
    
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)