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
import pandas as pd
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
    # Don't exit, just log the error
    logger.warning("Continuing despite directory creation error")

# Initialize Flask app with explicit template and static folder paths
app = Flask(__name__,
            template_folder=template_dir,
            static_folder=static_dir)

# Configure SQLAlchemy
database_url = os.environ.get('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    # Render uses postgres:// but SQLAlchemy requires postgresql://
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
    logger.info("Using PostgreSQL database")
else:
    # Default to SQLite for local development
    database_url = f'sqlite:///{os.path.join(BASE_DIR, "leads.db")}'
    logger.info("Using SQLite database")

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Use environment variable for secret key with a fallback
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-please-change-in-production')
# Disable debug mode in production
app.debug = os.environ.get('FLASK_ENV') == 'development'

# Initialize the database
db.init_app(app)

# Create database tables
try:
    with app.app_context():
        db.create_all()
        logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Error creating database tables: {e}")

def generate_sample_data(num_samples=15):
    """Generate simulated building permit data."""
    project_types = [
        'Residential Building (House)',
        'Commercial Building',
        'Industrial Building',
        'Residential Alteration',
        'Non-Residential Alteration'
    ]
    
    work_types = [
        'New Construction',
        'Addition to Building',
        'Interior Alteration',
        'Exterior Alteration',
        'Renovation'
    ]
    
    sub_work_types = [
        'Single Detached Dwelling',
        'Commercial',
        'Industrial',
        'Multi-Unit Residential',
        'Office Building'
    ]
    
    locations = [
        'Toronto, ON', 'Vancouver, BC', 'Montreal, QC', 'Calgary, AB',
        'Ottawa, ON', 'Edmonton, AB', 'Winnipeg, MB', 'Quebec City, QC',
        'Hamilton, ON', 'Victoria, BC', 'Halifax, NS', 'Saskatoon, SK'
    ]
    
    statuses = ['Pending', 'Approved', 'In Review', 'Closed', 'Expired']
    
    # Generate random dates within the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    sample_data = []
    for i in range(num_samples):
        submission_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Generate random coordinates in Canada
        lat = random.uniform(43.0, 50.0)
        lon = random.uniform(-123.0, -79.0)
        
        # Generate random construction value
        construction_value = random.randint(10000, 2000000)
        
        lead = Lead(
            permit_number=f'BP{datetime.now().year}{i+1:04d}',
            project_type=random.choice(project_types),
            permit_status=random.choice(statuses),
            permit_description=f'New {random.choice(sub_work_types).lower()} project',
            submission_date=submission_date,
            issue_date=submission_date + timedelta(days=random.randint(5, 15)) if random.random() > 0.3 else None,
            expiry_date=submission_date + timedelta(days=365),
            location=random.choice(locations),
            work_type=random.choice(work_types),
            sub_work_type=random.choice(sub_work_types),
            construction_value=construction_value,
            total_units=random.randint(1, 10),
            units_created=random.randint(0, 5),
            legal_description=f'PLAN {random.randint(1000, 9999)} LOT {random.randint(1, 999)}',
            latitude=lat,
            longitude=lon,
            owner=f'Owner {i+1}',
            applicant=f'Applicant {i+1}',
            contractor=f'Contractor {i+1}',
            contractor_contact=f'contact{i+1}@example.com'
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
    try:
        # Read the cleaned CSV file
        df = pd.read_csv('cleaned_kitchener_permits.csv')
        
        # Apply filters from query parameters
        project_type = request.args.get('project_type')
        location = request.args.get('location')
        status = request.args.get('status')
        work_type = request.args.get('work_type')
        min_value = request.args.get('min_value', type=float)
        max_value = request.args.get('max_value', type=float)
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')

        # Convert date columns to datetime
        df['submission_date'] = pd.to_datetime(df['submission_date'])
        
        # Apply filters if they exist
        if project_type:
            df = df[df['project_type'] == project_type]
        if location:
            df = df[df['location'].str.contains(location, case=False, na=False)]
        if status:
            df = df[df['permit_status'] == status]
        if work_type:
            df = df[df['work_type'] == work_type]
        if min_value:
            df = df[df['construction_value'] >= min_value]
        if max_value:
            df = df[df['construction_value'] <= max_value]
        if date_from:
            df = df[df['submission_date'] >= date_from]
        if date_to:
            df = df[df['submission_date'] <= date_to]

        # Calculate statistics
        total_leads = len(df)
        total_value = f"${df['construction_value'].sum():,.2f}"
        avg_value = f"${df['construction_value'].mean():,.2f}"

        # Get unique values for filters
        available_filters = {
            'project_types': sorted(df['project_type'].unique().tolist()),
            'locations': sorted(df['location'].unique().tolist()),
            'statuses': sorted(df['permit_status'].unique().tolist()),
            'work_types': sorted(df['work_type'].unique().tolist()),
            'value_range': {
                'min': df['construction_value'].min(),
                'max': df['construction_value'].max()
            }
        }

        # Calculate statistics by category
        project_types = df['project_type'].value_counts().to_dict()
        work_types = df['work_type'].value_counts().to_dict()
        statuses = df['permit_status'].value_counts().to_dict()

        # Convert DataFrame to list of dictionaries for template
        leads = df.to_dict('records')

        return render_template('dashboard.html',
                             leads=leads,
                             total_leads=total_leads,
                             total_value=total_value,
                             avg_value=avg_value,
                             project_types=project_types,
                             work_types=work_types,
                             statuses=statuses,
                             available_filters=available_filters)

    except FileNotFoundError:
        flash('No permit data found. Please ensure the data file exists.', 'error')
        return render_template('dashboard.html', leads=[])
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dashboard.html', leads=[])

@app.route('/download-csv')
def download_csv():
    try:
        # Apply filters from query parameters
        df = pd.read_csv('cleaned_kitchener_permits.csv')
        
        # Apply the same filters as the dashboard
        project_type = request.args.get('project_type')
        location = request.args.get('location')
        status = request.args.get('status')
        work_type = request.args.get('work_type')
        min_value = request.args.get('min_value', type=float)
        max_value = request.args.get('max_value', type=float)
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')

        if project_type:
            df = df[df['project_type'] == project_type]
        if location:
            df = df[df['location'].str.contains(location, case=False, na=False)]
        if status:
            df = df[df['permit_status'] == status]
        if work_type:
            df = df[df['work_type'] == work_type]
        if min_value:
            df = df[df['construction_value'] >= min_value]
        if max_value:
            df = df[df['construction_value'] <= max_value]
        if date_from:
            df = df[df['submission_date'] >= date_from]
        if date_to:
            df = df[df['submission_date'] <= date_to]

        # Create a temporary file for download
        temp_file = 'filtered_permits.csv'
        df.to_csv(temp_file, index=False)
        
        return send_file(temp_file,
                        mimetype='text/csv',
                        as_attachment=True,
                        download_name='filtered_permits.csv')
    except Exception as e:
        flash(f'Error downloading CSV: {str(e)}', 'error')
        return redirect(url_for('dashboard'))

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

@app.route('/api/leads/<int:lead_id>')
def get_lead_details(lead_id):
    try:
        df = pd.read_csv('cleaned_kitchener_permits.csv')
        lead = df[df['permit_number'] == lead_id].to_dict('records')[0]
        return jsonify(lead)
    except Exception as e:
        return jsonify({'error': str(e)}), 404

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'template_dir': os.path.exists(template_dir),
        'static_dir': os.path.exists(static_dir),
        'database': 'connected' if db.engine.execute('SELECT 1').scalar() else 'error'
    })

if __name__ == '__main__':
    # Log startup information
    logger.info(f"Starting application in {os.environ.get('FLASK_ENV', 'production')} mode")
    logger.info(f"Template directory exists: {os.path.exists(template_dir)}")
    logger.info(f"Static directory exists: {os.path.exists(static_dir)}")
    
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)